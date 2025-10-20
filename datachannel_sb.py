from dataclasses import dataclass, asdict
from typing import Optional
import urllib.parse
import logging
import json
import time
import os
import pickle
import base64
import hashlib

import asyncio
import uuid
from aiortc import RTCDataChannel, RTCPeerConnection, RTCSessionDescription, RTCConfiguration
from supabase import acreate_client, AsyncClient
from realtime import AsyncRealtimeChannel

signal_room = os.environ.get('SIGNAL_ROOM', 'default')
endpoint_domain = os.environ.get('ENDPOINT_DOMAIN', None)
supabase_key = os.environ.get('SUPABASE_KEY', '')
supabase_url = os.environ.get('SUPABASE_URL', '')

DEFAULT_CONFIG = RTCConfiguration()
RELAY_BUFFER_SIZE = 4096
BOOTSTRAP_LABEL = 'bootstrap'
TRANSPORT_LABEL = 'transport'

logger = logging.getLogger('tunnel')


class ProxyPeer:
    ''' proxy peer base class '''
    peer_id: str = None

    async def start(self):
        pass

    async def stop(self):
        pass

    def connected(self):
        return True

    async def recover(self):
        pass

    async def do_request(self, req: 'LocalRequest', timeout: int = 60):
        pass


@dataclass
class TransferData:
    ''' relay data with tid send/receive through one channel '''
    ''' mux multiple tcp connections through one data channel '''
    tid: int
    data: bytes

    @classmethod
    def from_bytes(cls, data: bytes):
        pickled = pickle.loads(data)
        return cls(pickled.tid, pickled.data)

    def to_bytes(self):
        return pickle.dumps(self)


@dataclass
class LocalRequest:
    ''' request send to event loop '''

    method: str = None
    "The type of the event"

    uri: Optional[str] = None
    "request uri"

    tid: str = str(uuid.uuid1())
    "transaction id"

    reader: Optional[asyncio.StreamReader] = None

    writer: Optional[asyncio.StreamWriter] = None

    future: Optional[asyncio.Future] = None


@dataclass
class ChannelMessage:
    ''' message over supabase realtime '''

    id: Optional[str] = None
    'sender id, auto increment'

    type: Optional[str] = None
    'message type'

    content: Optional[dict] = None
    'message content'

    timestamp: Optional[str] = None
    'message timestamp'

    sender: Optional[str] = None
    'sender display name'

    def asdict(self) -> dict:
        self_dict = asdict(self)
        return {k: v for k, v in self_dict.items() if v is not None}

    @classmethod
    def fromdict(cls, d: dict):
        return cls(**d)


@dataclass
class ChannelCommand:
    ''' rpc request/response over supabase realtime or data channel '''

    tid: Optional[str] = None
    'transaction id, none for response'

    method: Optional[str] = None
    'rpc method name, none for response'

    body: Optional[str] = None
    'rpc request/response content'

    error: Optional[str] = None
    'rpc response error'

    future: Optional[asyncio.Future] = None
    'rpc request future holder, not serialized'

    def asdict(self) -> dict:
        self_dict = {
            'tid': self.tid,
            'method': self.method,
            'body': self.body,
            'error': self.error,
        }
        return {k: v for k, v in self_dict.items() if v is not None}

    @classmethod
    def fromdict(cls, d: dict):
        return cls(**d)

    def to_json(self) -> str:
        return json.dumps(self.asdict())

    @classmethod
    def from_json(cls, json_str: str):
        self_dict = json.loads(json_str)
        return cls(**self_dict)


class DataChannelPair:
    ''' A pair of data channels '''
    ''' when in relay mode, sender and receiver are different '''
    ''' in p2p mode, sender and receiver are the same '''

    def __init__(self,
                 signal_sid: str,
                 local_sid: str,
                 remote_sid: str,
                 sender: RTCDataChannel,
                 receiver: RTCDataChannel):
        ''' signal_sid mark this pair owned by which signal session '''
        self.signal_sid = signal_sid
        self.local_sid = local_sid
        self.remote_sid = remote_sid
        self.sender = sender
        self.receiver = receiver

        if not sender or not receiver:
            self.close()
            return

        @sender.on('close')
        def on_sender_close():
            log(self.signal_sid, f'dc {self.sender.label} sender close')
            receiver.close()

        @receiver.on('close')
        def on_receiver_close():
            log(self.signal_sid, f'dc {self.sender.label} receiver close')
            sender.close()

    @classmethod
    def new_p2p(cls, sid, dc: RTCDataChannel):
        ''' create a p2p pair with same sender and receiver '''
        return cls(sid, sid, sid, dc, dc)

    def get_label(self):
        return self.receiver.label

    def create_pair(self, sender: RTCDataChannel, receiver: RTCDataChannel):
        return DataChannelPair(self.signal_sid, self.local_sid, self.remote_sid, sender, receiver)

    def close(self):
        if self.sender:
            self.sender.close()
        if self.receiver:
            self.receiver.close()

    def is_valid(self):
        return self.sender and self.receiver

    def is_ready(self):
        if not self.sender or not self.receiver:
            return False
        return self.sender.readyState == 'open' and self.receiver.readyState == 'open'

    def get_pair(self):
        return self.sender, self.receiver

    def get_sid_pair(self):
        return self.local_sid, self.remote_sid

    def send(self, data):
        ''' send data without wrapper '''
        if self.is_ready():
            self.sender.send(data)

    def transfer(self, tid, data):
        ''' transfer data with tid '''
        if self.is_ready():
            transfer_data = TransferData(tid, data)
            dumps = transfer_data.to_bytes()
            self.sender.send(dumps)


def log(trace_tag: str, msg: str):
    logger.info(msg=f'{trace_tag} {msg}')


def _reject(request: LocalRequest, msg: str = 'Not ready'):
    if request.writer:
        request.writer.write(f'HTTP/1.1 500 {msg}\r\n\r\n'.encode())
        safe_close(request.writer)
    log(f'{request.tid} {request.uri}', f'rejected: {msg}')


async def safe_close_peer(pc: RTCPeerConnection):
    try:
        if pc:
            await pc.close()
    except Exception:
        pass


def safe_close(writer: asyncio.StreamWriter):
    ''' Safely close a StreamWriter '''
    try:
        if writer and not writer.is_closing():
            writer.close()
    except Exception:
        pass


async def safe_write(writer: asyncio.StreamWriter, data: bytes):
    ''' Safely write data to a StreamWriter '''
    try:
        if writer and len(data) and not writer.is_closing():
            writer.write(data)
            await writer.drain()
    except Exception:
        print('safe_write error')
        safe_close(writer)


async def safe_write_buffers(writer: asyncio.StreamWriter, buffers: list[bytes]):
    ''' Safely write all buffers to a StreamWriter '''
    if not writer or writer.is_closing():
        return
    try:
        for b in buffers:
            writer.write(b)
        await writer.drain()
        buffers.clear()
    except Exception:
        print('safe_write_buffers error')
        safe_close(writer)


async def relay_reader_to_dc(reader: asyncio.StreamReader, pair: DataChannelPair, tid: str = None):
    ''' Relay data from StreamReader to DataChannel '''
    while not reader.at_eof():
        if (pair.sender.bufferedAmount > RELAY_BUFFER_SIZE * 100):
            await asyncio.sleep(1)
            continue
        data = await reader.read(RELAY_BUFFER_SIZE)
        if not data:
            break
        if tid:
            pair.transfer(tid, data)
        else:
            pair.send(data)
    log(pair.signal_sid, f'dc {pair.get_label()} relays done')


class RealtimePeer(ProxyPeer):
    ''' A peer that uses supabase realtime as signaling server '''

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._outgoing_requests: dict[str, ChannelCommand] = {}
        self.peer_id = str(uuid.uuid1())
        self.client: AsyncClient = None
        self.channel: AsyncRealtimeChannel = None
        self.peers: dict[str, RTCPeerConnection] = {}
        self._peers_lock = asyncio.Lock()

    async def relay_server_handler(self, transport: DataChannelPair, netloc: str, tid: str = None):
        writer = None
        try:
            host, port = netloc.split(':')
            if not host or not port:
                raise RuntimeError('invalid label')

            buffers = bytearray()

            @transport.receiver.on('message')
            def on_message(msg):
                if isinstance(msg, str):
                    return
                if tid is None:
                    if writer is None:
                        buffers.extend(msg)
                    else:
                        asyncio.ensure_future(safe_write(writer, msg))
                    return
                else:
                    tdata = TransferData.from_bytes(msg)
                    if tdata.tid == tid:
                        asyncio.ensure_future(safe_write(writer, tdata.data))

            reader, writer = await asyncio.open_connection(host, port)
            log(transport.signal_sid, f'dc {transport.get_label()} connected to {host}:{port}')

            await safe_write(writer, buffers)
            await relay_reader_to_dc(reader, transport)
        except Exception:
            pass
        finally:
            transport.close()
            safe_close(writer)

    ''' Create a new RTCPeerConnection with a bootstrap DataChannel '''
    async def create_peer(self, peer_id: str, opened=None, closed=None):
        await safe_close_peer(self.peers.get(peer_id, None))
        log(peer_id, f'create new peer')
        self.peers[peer_id] = pc = RTCPeerConnection(DEFAULT_CONFIG)

        @pc.on('connectionstatechange')
        def on_connection_state():
            log(peer_id, f'{pc.connectionState}')
            if pc.connectionState == 'failed':
                asyncio.ensure_future(safe_close_peer(pc))
            elif pc.connectionState == 'closed':
                self.peers.pop(peer_id, None)

        def on_open():
            log(peer_id, f'dc bootstrap opened')
            if opened:
                opened()

        def on_close():
            log(peer_id, f'dc bootstrap closed')
            self.peers.pop(peer_id, None)
            if closed:
                closed()

        dc = pc.createDataChannel(BOOTSTRAP_LABEL)
        dc.once('open', on_open)
        dc.once('close', on_close)

        await pc.setLocalDescription(await pc.createOffer())
        return pc

    async def new_server_peer(self, req: ChannelCommand):
        pc = await self.create_peer(req.tid)

        @pc.on('datachannel')
        def on_datachannel(dc: RTCDataChannel):
            if dc.label == BOOTSTRAP_LABEL:
                return
            p2p_transport = DataChannelPair.new_p2p(req.tid, dc)
            asyncio.create_task(self.relay_server_handler(p2p_transport, dc.label))

        res = ChannelCommand(req.tid, body={
            'ice': [],
            'answer': pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:active')
        })
        await pc.setRemoteDescription(RTCSessionDescription(
            req.body['offer'].replace('a=setup:actpass', 'a=setup:passive'), 'answer'))
        return res

    async def handle_channel_request(self, req: ChannelCommand):
        if req.method == 'ping':
            await self.send_channel_command(ChannelCommand(req.tid, body='pong'))
        elif req.method == 'connect':
            # TODO handle new peer request in random delay
            res = await self.new_server_peer(req)
            await self.send_channel_command(res)

    def _recv_channel_command(self, msg):
        try:
            channelMessage = ChannelMessage.fromdict(msg['payload'])
            if self.peer_id == channelMessage.id:
                # command from self, ignore
                return

            cmd: ChannelCommand = ChannelCommand.fromdict(channelMessage.content)
            if not cmd.tid:
                log(self.peer_id, f'invalid command no tid {cmd}')
                return

            if cmd.tid in self._outgoing_requests:
                log(cmd.tid, f'recv res {cmd}')
                # get response from other peer
                req = self._outgoing_requests.pop(cmd.tid)
                if req and req.future and not req.future.done():
                    req.future.set_result(cmd)
                return

            # handle incoming request
            asyncio.ensure_future(self.handle_channel_request(cmd))
        except Exception as e:
            log(self.peer_id, f'handle incoming message error {e}')

    async def send_channel_command(self, req: ChannelCommand, timeout: int = 30):
        if req is None:
            return
        if not self.connected():
            req.future.set_exception(Exception('channel not ready'))
            return

        msg = ChannelMessage(self.peer_id, 'command', req.asdict(), str(time.time()))
        await self.channel.send_broadcast('command', msg.asdict())

    async def channel_request(self, req: ChannelCommand, timeout: int = 30) -> ChannelCommand:
        if not req.tid:
            req.tid = str(uuid.uuid1())

        self._outgoing_requests[req.tid] = req
        req.future = asyncio.Future()
        try:
            await self.send_channel_command(req)
            await asyncio.wait_for(req.future, timeout)
            return req.future.result()
        except Exception as e:
            log(self.peer_id, f'channel request error {e.__class__.__name__} {e}')
            return None
        finally:
            self._outgoing_requests.pop(req.tid, None)

    async def new_transport(self, req: LocalRequest):
        return await self.new_client_peer(req)

    async def new_client_peer(self, req: LocalRequest):
        tid = req.tid
        peer_id = self.peer_id

        def on_open():
            def on_new_dc_open():
                log(tid, f'dc {new_dc.label} opened')
                if req.future.done():
                    return
                req.future.set_result(DataChannelPair.new_p2p(tid, new_dc))

            new_dc = peer.createDataChannel(req.uri)
            new_dc.once('open', on_new_dc_open)

        def on_close():
            if not req.future.done():
                req.future.set_exception(Exception('peer closed'))

        peer: RTCPeerConnection = self.peers.get(peer_id, None)
        if peer:
            if peer.connectionState == 'connected':
                on_open()
            elif not req.future.done():
                req.future.set_exception(Exception(f'peer state {peer.connectionState}'))
            return

        # create new peer connection
        peer = await self.create_peer(peer_id, opened=on_open, closed=on_close)
        res = await self.channel_request(ChannelCommand(method='connect', body={
            'offer': peer.localDescription.sdp, 'ice': []
        }))

        if not res:
            req.future.set_result(None)
            log(tid, f'channel request had no response, close peer')
            await safe_close_peer(peer)
            return

        if res.error:
            req.future.set_exception(Exception(res.error))
            log(tid, f'channel request error {res.error}, close peer')
            await safe_close_peer(peer)
            return

        if not res.body or 'answer' not in res.body:
            req.future.set_exception(Exception('invalid response'))
            log(tid, f'channel request had invalid response, close peer')
            await safe_close_peer(peer)
            return

        await peer.setRemoteDescription(RTCSessionDescription(res.body['answer'], 'answer'))

    async def ping_test(self, req: LocalRequest):
        res = await self.channel_request(ChannelCommand(method='ping'), 10)
        req.future.set_result(res.body if res else 'no response')
        log(self.peer_id, f'ping test result {res}')

    async def handle_http(self, req: LocalRequest):
        headers = await req.reader.readuntil(b'\r\n\r\n')
        netloc = req.uri
        if (req.uri.startswith('http://')):
            url = urllib.parse.urlparse(req.uri)
            netloc = f'{url.hostname}:{url.port or 80}'
        transport_req = LocalRequest('transport', netloc, req.tid)
        transport: DataChannelPair = await self.do_request(transport_req)

        if not transport or not transport.is_ready():
            log(self.peer_id, f'transport not ready {req.uri}')
            _reject(req)
            return

        @transport.receiver.on('message')
        def on_message(msg):
            if isinstance(msg, str):
                return
            asyncio.ensure_future(safe_write(req.writer, msg))

        @transport.receiver.on('close')
        def on_close():
            safe_close(req.writer)

        try:
            if req.method == 'CONNECT':
                http200 = b'HTTP/1.1 200 Connection established\r\n\r\n'
                asyncio.ensure_future(safe_write(req.writer, http200))
            else:
                req_line = f'{req.method} {req.uri} HTTP/1.1\r\n'.encode()
                transport.send(req_line)
                transport.send(headers)

            await relay_reader_to_dc(req.reader, transport)
        finally:
            transport.close()
            # make request done
            if not req.future.done():
                req.future.set_result(None)

    async def start_event_loop(self):
        log(self.peer_id, f'eventloop start')
        try:
            req: LocalRequest
            while True:
                req = await self._queue.get()
                if req.method == 'transport':
                    asyncio.create_task(self.new_transport(req))
                elif req.method == 'ping':
                    asyncio.create_task(self.ping_test(req))
                elif req.method == 'close':
                    break
                else:
                    asyncio.create_task(self.handle_http(req))
            log(self.peer_id, f'eventloop exit')
        except Exception as e:
            log(self.peer_id, f'eventloop error: {e}')

    async def do_request(self, req: LocalRequest, timeout: int = 60):
        trace_tag = f'{req.tid} {req.uri}'
        req.future = asyncio.Future()
        self._queue.put_nowait(req)
        try:
            await asyncio.wait_for(req.future, timeout)
            return req.future.result()
        except Exception as e:
            log(trace_tag, f'request {req.method} error {e.__class__.__name__} {e}')
            return None

    async def recover(self):
        if self.channel and self.channel.state == 'closed':
            await self.channel.subscribe()
            await self.channel.track({
                'id': self.peer_id,
                'name': 'RealtimePeer',
            })

    def connected(self):
        return self.channel and self.channel.state == 'joined'

    async def start(self):
        if self.client:
            return
        self.client = await acreate_client(supabase_url, supabase_key)
        self.channel = self.client.channel(f'room:{signal_room}:messages')
        self.channel.on_broadcast('command', self._recv_channel_command)
        await self.channel.subscribe()
        await self.channel.track({
            'id': self.peer_id,
            'name': 'RealtimePeer',
        })
        asyncio.create_task(self.start_event_loop())

    async def stop(self):
        if self.client:
            self.client.remove_all_channels()
        self._queue.put_nowait(LocalRequest('close'))
        await asyncio.gather(*[peer.close() for peer in self.peers.values()])


class HttpPeer(ProxyPeer):
    ''' http peer, use OPTIONS as CONNECT request '''

    WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

    def __init__(self):
        self.endpoint_cname = self._get_endpoint_cname()
        self.peer_id = 'http-peer'

    def _get_endpoint_cname(self):
        ''' get endpoint domain cname '''
        if not endpoint_domain:
            return None
        # request dns cname
        try:
            import dns.resolver
            answers = dns.resolver.resolve(endpoint_domain, 'CNAME', raise_on_no_answer=False)
            for r in answers:
                cname = str(r.target).rstrip('.')
                log('', f'get_endpoint_cname(dnspython) {cname}')
                return cname
        except Exception:
            return None

    async def relay_tcp_to_ws(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tag: str):
        ''' Relay data from StreamReader to ws StreamWriter '''
        while not reader.at_eof():
            payload = await reader.read(RELAY_BUFFER_SIZE)
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
        # Support both text (opcode 0x1) and binary (opcode 0x2) frames
        while True:
            data = await reader.read(2)
            if not data:
                break
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
                payload.extend(await reader.read(payload_len - len(payload)))

            if (len(payload) < payload_len):
                log(tag, f'ws->tcp read incomplete payload {len(payload)}/{payload_len}')
                break

            # Only relay text or binary frames
            if opcode in (0x01, 0x02):
                # log(tag, f'ws->tcp write {payload_len} bytes {len(payload)}')
                await safe_write(writer, payload)
        safe_close(writer)
        log(tag, 'ws->tcp relays done')

    async def do_connect(self, req: LocalRequest, timeout):
        trace_tag = f'{req.tid} {req.uri}'
        reader, writer = None, None
        try:
            # ignore all headers
            await asyncio.wait_for(req.reader.readuntil(b'\r\n\r\n'), timeout=timeout)
            # open connection to remote http endpoint
            reader, writer = await asyncio.open_connection(self.endpoint_cname, 443, ssl=True)

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
                _reject(req, 'Server failed')
                safe_close(writer)
                return

            # remote connect success, reply http200 to client
            http200 = b'HTTP/1.1 200 Connection established\r\n\r\n'
            await safe_write(req.writer, http200)

            asyncio.ensure_future(self.safe_tcp_to_ws(req.reader, writer, trace_tag))
            await self.safe_ws_to_tcp(reader, req.writer, trace_tag)
        except Exception as e:
            log(trace_tag, f'request CONNECT failed: {e}')
            self.endpoint_cname = self._get_endpoint_cname()
            _reject(req, 'Connection failed')
            safe_close(writer)

    async def do_websocket(self, req: LocalRequest, timeout: int = 60):
        trace_tag = f'{req.tid} {req.uri}'
        try:
            headers = await asyncio.wait_for(req.reader.readuntil(b'\r\n\r\n'), timeout=timeout)
            netloc = req.uri.lstrip('/connect/')
            host, port = netloc.split(':')
            if not host or not port:
                _reject(req, 'Invalid host')

            trace_tag = f'{req.tid} {netloc}'
            # parse websocket key from headers, and calculate accept key
            ws_key = None
            header_lines = headers.decode().split('\r\n')
            for line in header_lines:
                if line.lower().startswith('sec-websocket-key:'):
                    ws_key = line.split(':', 1)[1].strip()
                    break
            if not ws_key:
                _reject(req, 'Invalid key')
                return
            ws_accept = base64.b64encode(hashlib.sha1((ws_key + self.WS_MAGIC).encode()).digest()).decode()

            reader, writer = await asyncio.open_connection(host, port)
            # remote connect success, reply http101 to client
            response = (
                'HTTP/1.1 101 Switching Protocols\r\n'
                'Connection: upgrade\r\n'
                'Upgrade: websocket\r\n'
                f'Sec-WebSocket-Accept: {ws_accept}\r\n\r\n'
            )
            await safe_write(req.writer, response.encode())

            log(self.peer_id, f'connected to {host}:{port}')
            asyncio.ensure_future(self.safe_ws_to_tcp(req.reader, writer, netloc))
            await self.safe_tcp_to_ws(reader, req.writer, netloc)
        except Exception as e:
            log(trace_tag, f'request WEBSOCKET failed: {e}')
            _reject(req, 'Connection failed')

    async def do_request(self, req: LocalRequest, timeout: int = 10):
        if req.method == 'CONNECT':
            await self.do_connect(req, timeout)
        elif req.method == 'GET' and req.uri.startswith('/connect/'):
            await self.do_websocket(req, timeout)
        else:
            _reject(req, 'Not supported')


class HttpServer:
    ''' http proxy '''

    def __init__(self, endpoint: ProxyPeer):
        self.logger = logging.getLogger('http')
        self.realtime_peer = endpoint
        self.http_peer = HttpPeer()
        self.switch_peer = False

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_addr, client_port = writer.get_extra_info('peername')
        tag = f'{client_addr}:{client_port}'
        self.logger.info(f'{tag} new connection')
        try:
            request_line = (await reader.readline()).decode()
            method, uri, _ = request_line.split()
        except Exception:
            writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
            safe_close(writer)
            return

        # random switch between realtime and http peer
        self.switch_peer = not self.switch_peer
        peer = self.realtime_peer if self.switch_peer else self.http_peer
        self.logger.info(f'{tag} received {method} {uri} by {peer.peer_id}')
        # endpoint readiness check
        if method == 'GET' and uri == '/':
            if self.realtime_peer.connected():
                writer.write(b'HTTP/1.1 200 OK\r\n\r\n')
            else:
                writer.write(b'HTTP/1.1 500 Not ready\r\n\r\n')
            safe_close(writer)
            return
        # custom testing
        if method == 'GET' and uri == '/ping':
            res = await self.realtime_peer.do_request(LocalRequest('ping'))
            writer.write(f'HTTP/1.1 200 {res}\r\n\r\n'.encode())
            safe_close(writer)
            return
        # proxy over websocket
        if method == 'GET' and uri.startswith('/connect/'):
            await self.http_peer.do_request(LocalRequest(
                method,
                uri,
                self.http_peer.peer_id,
                reader,
                writer
            ))
            # recover realtime peer if needed
            if not self.realtime_peer.connected():
                await self.realtime_peer.recover()
            return

        # https proxy
        if method != 'CONNECT':
            # http proxy, request line: GET http://host:port/path HTTP/1.1
            if not uri.startswith('http://'):
                writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
                safe_close(writer)
                return

        if peer.connected():
            try:
                req = LocalRequest(
                    method.upper(),
                    uri,
                    tag,
                    reader,
                    writer
                )
                await peer.do_request(req, timeout=300)
                self.logger.info(f'{tag} done {method} {uri}')
            except Exception as e:
                self.logger.error(f'{tag} error {method} {uri}: {e}')
                writer.write(b'HTTP/1.1 500 Internal Server Error\r\n\r\n')
                safe_close(writer)
        else:
            self.logger.info(f'{tag} rejected {method} {uri}')
            writer.write(b'HTTP/1.1 500 Not ready\r\n\r\n')
            safe_close(writer)
            # recover peer
            await peer.recover()

    async def start(self, port: int = 1234):
        server = await asyncio.start_server(self.handle_request, port=port)
        addr = server.sockets[0].getsockname()
        self.logger.info(f'Local server running on {addr[0]}:{addr[1]}')

        await self.realtime_peer.start()
        async with server:
            await server.serve_forever()

    async def stop(self):
        if self.realtime_peer:
            await self.realtime_peer.stop()


async def main():
    process = None
    try:
        if '--server' in sys.argv:
            process = HttpServer(RealtimePeer())
            await process.start(port=8001)
        elif '--http' in sys.argv:
            process = HttpServer(HttpPeer())
            await process.start(port=2234)
        else:
            process = HttpServer(RealtimePeer())
            await process.start(port=2234)
    except KeyboardInterrupt:
        pass
    finally:
        await process.stop()

if __name__ == '__main__':
    import sys
    from aioice import ice
    ice.CONSENT_INTERVAL = 2
    ice.CONSENT_FAILURES = 5
    # Setup logging configuration
    debug = '--debug' in sys.argv
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    if not debug:
        logging.getLogger('realtime._async.client').setLevel(logging.WARNING)
    asyncio.run(main())
