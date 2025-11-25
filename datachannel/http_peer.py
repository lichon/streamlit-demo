import os
import base64
import hashlib
import json
import time

import asyncio
from proxy_peer import ProxyPeer, LocalRequest, log, safe_close, safe_write


async def get_cname_from_alidns(domain: str) -> str:
    '''Async get CNAME from AliDNS public resolver using only asyncio and stdlib'''
    host = '223.5.5.5'
    port = 443
    path = f'/resolve?name={domain}&type=CNAME'
    reader, writer = await asyncio.open_connection(host, port, ssl=True)
    req = (
        f'GET {path} HTTP/1.1\r\n'
        f'Host: dns.alidns.com\r\n'
        f'User-Agent: Python\r\n'
        f'Accept: application/json\r\n'
        f'Connection: close\r\n\r\n'
    )
    writer.write(req.encode())
    await writer.drain()

    response = b''
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            break
        response += chunk
    writer.close()
    await writer.wait_closed()

    # Split headers and body
    header_end = response.find(b'\r\n\r\n')
    if header_end == -1:
        return None
    body = response[header_end + 4:]
    try:
        data = json.loads(body.decode())
        for answer in data.get('Answer', []):
            if answer.get('type') == 5:  # 5 = CNAME
                return answer.get('data').rstrip('.')
    except Exception:
        return None
    return None


class HttpPeer(ProxyPeer):
    ''' http peer, use OPTIONS as CONNECT request '''

    RELAY_BUFFER_SIZE = 4096
    WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
    CF_HTTPS_PORTS = (443, 2053, 2083, 2087, 2096, 8443)

    def __init__(self):
        self.https_port_idx = 0
        self.endpoint_cname = os.environ.get('ENDPOINT_DOMAIN', 'localhost')
        asyncio.ensure_future(self._get_endpoint_cname())

    async def _get_endpoint_cname(self):
        ''' get endpoint domain cname '''
        endpoint_domain = os.environ.get('ENDPOINT_DOMAIN', 'localhost')
        try:
            self.endpoint_cname = await get_cname_from_alidns(endpoint_domain)
            log('', f'get_endpoint_cname(alidns) {self.endpoint_cname}')
        except Exception as e2:
            log('', f'get_endpoint_cname(alidns) error {e2}')

    async def relay_tcp_to_ws(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tag: str):
        ''' Relay data from StreamReader to ws StreamWriter '''
        while not reader.at_eof():
            payload = await reader.read(self.RELAY_BUFFER_SIZE)
            if not payload:
                break
            payload_len = len(payload)

            frame = bytearray()
            frame.append(0x80 | 0x02)

            if payload_len < 126:
                frame.append(payload_len)
            elif payload_len <= 0xFFFF:
                frame.append(126)
                frame.extend(payload_len.to_bytes(2, 'big'))
            else:
                frame.append(127)
                frame.extend(payload_len.to_bytes(8, 'big'))

            frame.extend(payload)
            # log(tag, f'tcp->ws write {payload_len} bytes {len(payload)}')
            await safe_write(writer, bytes(frame))
        safe_close(writer)
        log(tag, 'tcp->ws relays done')

    async def safe_ws_to_tcp(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tag: str):
        try:
            await self.relay_ws_to_tcp(reader, writer, tag)
        except Exception as e:
            log(tag, f'ws->tcp relay error {e}')

    async def safe_tcp_to_ws(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tag: str):
        try:
            await self.relay_tcp_to_ws(reader, writer, tag)
        except Exception as e:
            log(tag, f'tcp->ws relay error {e}')

    async def relay_ws_to_tcp(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tag: str):
        ''' Relay data from ws StreamReader to StreamWriter '''
        enable_rate_limit = 'github' in tag or 'google.com' in tag
        packets_sent = 0
        start_time = time.monotonic()
        # Support both text (opcode 0x1) and binary (opcode 0x2) frames
        while True:
            current_time = time.monotonic()
            if current_time - start_time >= 1.0:
                packets_sent = 0
                start_time = current_time

            # 250 buffer per second rate limit
            if enable_rate_limit and packets_sent >= 250:
                log(tag, f'ws->tcp rate limit reached, sleeping {start_time + 1.0 - current_time:.3f}s')
                await asyncio.sleep(start_time + 1.0 - current_time)
                packets_sent = 0
                start_time = time.monotonic()
                continue
            data = await reader.read(2)
            if not data or len(data) < 2:
                break
            packets_sent += 1
            fin_opcode = data[0]
            opcode = fin_opcode & 0x0F
            mask = (data[1] >> 7) & 0x01
            payload_len = data[1] & 127
            if payload_len == 126:
                ext = await reader.read(2)
                payload_len = int.from_bytes(ext, 'big')
            elif payload_len == 127:
                ext = await reader.read(8)
                payload_len = int.from_bytes(ext, 'big')

            payload = bytearray()
            if mask:
                mask_key = await reader.read(4)
                encoded = await reader.read(payload_len)
                payload.extend(bytes(b ^ mask_key[i % 4] for i, b in enumerate(encoded)))
            else:
                payload.extend(await reader.read(payload_len))

            while len(payload) < payload_len:
                remain = await reader.read(payload_len - len(payload))
                if not remain:
                    break
                payload.extend(remain)

            if len(payload) < payload_len:
                log(tag, f'ws->tcp read incomplete payload {len(payload)}/{payload_len}')
                break

            # Only relay text or binary frames
            if opcode in (0x01, 0x02):
                # log(tag, f'ws->tcp write {payload_len} bytes {len(payload)}')
                await safe_write(writer, payload)
        safe_close(writer)
        log(tag, 'ws->tcp relays done')

    async def relay_tcp_to_tcp(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tag: str):
        ''' Relay data from tcp StreamReader to tcp StreamWriter '''
        while True:
            data = await reader.read(self.RELAY_BUFFER_SIZE)
            if not data:
                break
            await safe_write(writer, data)
        safe_close(writer)
        log(tag, 'tcp->tcp relays done')

    async def do_connect(self, req: LocalRequest, timeout):
        trace_tag = f'{req.tid} {req.uri}'
        reader, writer = None, None
        self.https_port_idx = 0 if self.https_port_idx >= len(self.CF_HTTPS_PORTS) - 1 else self.https_port_idx + 1
        http_port = self.CF_HTTPS_PORTS[self.https_port_idx]
        http_ssl = True
        if self.endpoint_cname == 'localhost':
            http_port = 2234
            http_ssl = False
        try:
            # ignore all headers
            await asyncio.wait_for(req.reader.readuntil(b'\r\n\r\n'), timeout=timeout)
            # open connection to remote http endpoint
            reader, writer = await asyncio.open_connection(self.endpoint_cname, http_port, ssl=http_ssl)

            log(trace_tag, f'connected to {self.endpoint_cname}:{http_port}')
            # relay connect request to remote endpoint
            req_headers = (
                f'GET /connect/{req.uri} HTTP/1.1\r\n'
                f'Host: {self.endpoint_cname}\r\n'
                f"Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==\r\n"
                f"Sec-WebSocket-Version: 13\r\n"
                f'Connection: upgrade\r\n'
                f'Upgrade: websocket\r\n\r\n'
            )
            await safe_write(writer, req_headers.encode())
            http101 = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 10)
            if not http101.decode().startswith('HTTP/1.1 101'):
                req.reject('Server failed')
                safe_close(writer)
                await self._get_endpoint_cname()
                return

            # remote connect success, reply http200 to client
            http200 = b'HTTP/1.1 200 Connection established\r\n\r\n'
            await safe_write(req.writer, http200)

            asyncio.ensure_future(self.safe_tcp_to_ws(req.reader, writer, trace_tag))
            await self.safe_ws_to_tcp(reader, req.writer, trace_tag)
        except Exception as e:
            log(trace_tag, f'request CONNECT failed: {e}')
            await self._get_endpoint_cname()
            req.reject('Connection failed')
            safe_close(writer)

    async def ws_accept(self, req: LocalRequest, headers: str):
        # parse websocket key from headers, and calculate accept key
        ws_key = None
        header_lines = headers.split('\r\n')
        for line in header_lines:
            if line.lower().startswith('sec-websocket-key:'):
                ws_key = line.split(':', 1)[1].strip()
                break
        if not ws_key:
            req.reject('Invalid key')
            return

        sec_accept = base64.b64encode(hashlib.sha1((ws_key + self.WS_MAGIC).encode()).digest()).decode()
        # reply http101 to accept ws
        response = (
            'HTTP/1.1 101 Switching Protocols\r\n'
            'Connection: upgrade\r\n'
            'Upgrade: websocket\r\n'
            f'Sec-WebSocket-Accept: {sec_accept}\r\n\r\n'
        )
        await safe_write(req.writer, response.encode())

    async def do_websocket(self, req: LocalRequest, timeout: int = 60):
        trace_tag = f'{req.tid} {req.uri}'
        try:
            headers = await asyncio.wait_for(req.reader.readuntil(b'\r\n\r\n'), timeout=timeout)
            netloc = req.uri.removeprefix('/connect/')
            host, port = netloc.split(':')
            if not host or not port:
                req.reject('Invalid host')

            reader, writer = await asyncio.open_connection(host, port)
            # remote connect success, reply http101 to client
            await self.ws_accept(req, headers.decode())

            log(trace_tag, f'connected to {host}:{port}')
            asyncio.ensure_future(self.safe_ws_to_tcp(req.reader, writer, netloc))
            await self.safe_tcp_to_ws(reader, req.writer, netloc)
        except Exception as e:
            log(trace_tag, f'do websocket failed: {e}')
            req.reject('Connection failed')

    def _get_original_netloc(self, req: LocalRequest) -> str:
        use_ssl = False
        uri = ''
        netloc = ''
        if (req.uri.startswith('/http/')):
            uri = req.uri.removeprefix('/http/')
            netloc = uri.split('/')[0]
            uri = uri[len(netloc):] or '/'
        if (req.uri.startswith('/https/')):
            uri = req.uri.removeprefix('/https/')
            netloc = uri.split('/')[0]
            uri = uri[len(netloc):] or '/'
            use_ssl = True
        if (req.uri.startswith('/r/')):
            uri = req.uri
            netloc = uri.split('/')[2]
        if (req.uri.startswith('/rs/')):
            uri = req.uri
            netloc = uri.split('/')[2]
            use_ssl = True
        return netloc, uri, use_ssl

    async def do_proxy(self, req: LocalRequest, timeout: int = 60):
        trace_tag = f'{req.tid} {req.uri}'
        http_ssl = False
        try:
            headers = await asyncio.wait_for(req.reader.readuntil(b'\r\n\r\n'), timeout=timeout)
            netloc, uri, http_ssl = self._get_original_netloc(req)
            log(trace_tag, f'{netloc} {uri} ssl={http_ssl}')
            host_port = netloc.split(':')
            if not host_port:
                req.reject('Invalid host')
                return

            host, port = host_port if len(host_port) == 2 else (host_port[0], 443 if http_ssl else 80)
            reader, writer = await asyncio.open_connection(host, port, ssl=http_ssl)

            writer.write(f'{req.method} {uri} HTTP/1.1\r\n'.encode())
            header_lines = headers.decode().split('\r\n')
            for line in header_lines:
                if line.lower().startswith('host:'):
                    writer.write(f'Host: {netloc}\r\n'.encode())
                else:
                    writer.write(line.encode() + b'\r\n')
            await writer.drain()

            log(trace_tag, f'connected to {netloc}')
            asyncio.ensure_future(self.relay_tcp_to_tcp(req.reader, writer, netloc))
            await self.relay_tcp_to_tcp(reader, req.writer, netloc)
        except Exception as e:
            log(trace_tag, f'do proxy failed: {e}')
            req.reject('Connection failed')

    async def do_request(self, req: LocalRequest, timeout: int = 10):
        if req.method == 'CONNECT':
            await self.do_connect(req, timeout)
        elif req.method == 'GET' and req.uri.startswith('/connect/'):
            await self.do_websocket(req, timeout)
        elif req.uri.startswith('/http/') or req.uri.startswith('/https/'):
            # request proxy
            await self.do_proxy(req)
        elif req.uri.startswith('/r/') or req.uri.startswith('/rs/'):
            # reverse proxy
            await self.do_proxy(req)
        else:
            req.reject('Not supported')
