from dataclasses import dataclass, asdict
from typing import Optional
import urllib.parse
import logging
import json
import time
import os
import pickle

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

logger = logging.getLogger('proxypeer')


class ProxyPeer:
    ''' proxy peer base class '''
    peer_id: str = None

    def start(self):
        pass

    def stop(self):
        pass

    def connected(self):
        return True

    async def do_request(self):
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


def log(trace_id: str, msg: str):
    logger.info(msg=f'{trace_id} {msg}')


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


async def relay_reader_to_writer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tag: str):
    ''' Relay data from StreamReader to StreamWriter '''
    while not reader.at_eof():
        data = await reader.read(RELAY_BUFFER_SIZE)
        if not data:
            break
        await safe_write(writer, data)
    safe_close(writer)
    log(tag, 'tcp relays done')


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
        self.peer_id: str = None
        self.client: AsyncClient = None
        self.channel: AsyncRealtimeChannel = None
        self.peers: dict[str, RTCPeerConnection] = {}

    async def relay_server_handler(self, transport: DataChannelPair, netloc: str, tid: str = None):
        try:
            host, port = netloc.split(':')
            if not host or not port:
                raise RuntimeError('invalid label')

            writer = None
            buffers = []

            @transport.receiver.on('message')
            def on_message(msg):
                if isinstance(msg, str):
                    return
                if tid is None:
                    if writer is None:
                        buffers.append(msg)
                    else:
                        asyncio.ensure_future(safe_write(writer, msg))
                    return
                else:
                    tdata = TransferData.from_bytes(msg)
                    if tdata.tid == tid:
                        asyncio.ensure_future(safe_write(writer, tdata.data))

            reader, writer = await asyncio.open_connection(host, port)
            log(transport.signal_sid, f'dc {transport.get_label()} connected to {host}:{port}')

            await safe_write_buffers(writer, buffers)
            asyncio.ensure_future(relay_reader_to_dc(reader, transport, tid))
        except Exception:
            pass

    ''' Create a new RTCPeerConnection with a bootstrap DataChannel '''
    async def create_peer(self, tid: str, opened=None, closed=None):
        # close old peer
        await safe_close_peer(self.peers.get(tid, None))

        def on_open():
            log(tid, f'dc bootstrap opened')
            if opened:
                opened()

        def on_close():
            log(tid, f'dc bootstrap closed')
            self.peers.pop(tid, None)
            if closed:
                closed()

        log(tid, f'create new peer {tid}')
        self.peers[tid] = pc = RTCPeerConnection(DEFAULT_CONFIG)

        @pc.on('connectionstatechange')
        def on_connection_state():
            log(tid, f'{pc.connectionState}')
            if pc.connectionState == 'failed':
                asyncio.ensure_future(safe_close_peer(pc))
            elif pc.connectionState == 'closed':
                self.peers.pop(tid, None)

        dc = pc.createDataChannel('bootstrap')
        dc.once('open', on_open)
        dc.once('close', on_close)

        await pc.setLocalDescription(await pc.createOffer())
        return pc

    async def handle_p2p_connect(self, req: ChannelCommand):
        pc = await self.create_peer(req.tid)

        @pc.on('datachannel')
        def on_datachannel(dc: RTCDataChannel):
            p2p_transport = DataChannelPair.new_p2p(req.tid, dc)
            asyncio.create_task(self.relay_server_handler(p2p_transport, dc.label))

        res = ChannelCommand(req.tid, body={
            'ice': [],
            'answer': pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:active')
        })
        await pc.setRemoteDescription(RTCSessionDescription(
            req.body['offer'].replace('a=setup:actpass', 'a=setup:passive'), 'answer'))
        return res

    async def handle_incoming_request(self, req: ChannelCommand):
        if req.method == 'ping':
            self._send_channel_command(ChannelCommand(req.tid, body='pong'))
        elif req.method == 'connect':
            # TODO handle new peer request in random delay
            res = await self.handle_p2p_connect(req)
            self._send_channel_command(res)

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
            asyncio.ensure_future(self.handle_incoming_request(cmd))
        except Exception as e:
            log(self.peer_id, f'handle incoming message error {e}')

    def _send_channel_command(self, req: ChannelCommand, timeout: int = 30):
        if req is None:
            return
        if not self.connected():
            req.future.set_exception(Exception('channel not ready'))
            return

        msg = ChannelMessage(self.peer_id, 'command', req.asdict(), str(time.time()))
        asyncio.ensure_future(self.channel.send_broadcast('command', msg.asdict()))

    async def channel_request(self, req: ChannelCommand, timeout: int = 30) -> ChannelCommand:
        if not req.tid:
            req.tid = str(uuid.uuid1())

        self._outgoing_requests[req.tid] = req
        req.future = asyncio.Future()
        try:
            self._send_channel_command(req)
            await asyncio.wait_for(req.future, timeout)
            return req.future.result()
        except Exception as e:
            log(self.peer_id, f'channel request error {e.__class__.__name__} {e}')
            return None
        finally:
            self._outgoing_requests.pop(req.tid, None)

    async def create_p2p_transport(self, req: LocalRequest):
        tid = req.tid

        def on_open():
            def on_new_dc_open():
                log(tid, f'dc {new_dc.label} opened')
                req.future.set_result(DataChannelPair.new_p2p(tid, new_dc))

            new_dc = peer.createDataChannel(req.uri)
            new_dc.once('open', on_new_dc_open)

        def on_close():
            if not req.future.done():
                req.future.set_exception(Exception('peer closed'))

        peer: RTCPeerConnection = self.peers.get(tid, None)
        if peer:
            if peer.connectionState == 'connected':
                on_open()
            else:
                req.future.set_exception(Exception(f'peer state {peer.connectionState}'))
            return

        # create new peer connection
        peer = await self.create_peer(tid, opened=on_open, closed=on_close)
        res = await self.channel_request(ChannelCommand(tid=tid, method='connect', body={
            'offer': peer.localDescription.sdp, 'ice': []
        }))

        if not res:
            req.future.set_result(None)
            log(self.peer_id, f'channel request had no response, close peer')
            await safe_close_peer(peer)
            return

        if res.error:
            req.future.set_exception(Exception(res.error))
            log(self.peer_id, f'channel request error {res.error}, close peer')
            await safe_close_peer(peer)
            return

        if not res.body or 'answer' not in res.body:
            req.future.set_exception(Exception('invalid response'))
            log(self.peer_id, f'channel request had invalid response, close peer')
            await safe_close_peer(peer)
            return

        await peer.setRemoteDescription(RTCSessionDescription(res.body['answer'], 'answer'))

    async def ping_test(self, req: LocalRequest):
        res = await self.channel_request(ChannelCommand(method='ping'), 10)
        req.future.set_result(res.body if res else 'no response')
        log(self.peer_id, f'ping test result {res}')

    def _reject(request: LocalRequest):
        if request.writer:
            request.writer.write(b'HTTP/1.1 500 Not ready\r\n\r\n')
            safe_close(request.writer)

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
            self._reject(req)
            return

        @transport.receiver.on('message')
        def on_message(msg):
            if isinstance(msg, str):
                return
            asyncio.ensure_future(safe_write(req.writer, msg))

        if req.method == 'CONNECT':
            http200 = b'HTTP/1.1 200 Connection established\r\n\r\n'
            asyncio.ensure_future(safe_write(req.writer, http200))
        else:
            req_line = f'{req.method} {req.uri} HTTP/1.1\r\n'.encode()
            transport.send(req_line)
            transport.send(headers)

        await relay_reader_to_dc(req.reader, transport)
        # make request done
        req.future.set_result(None)

    async def start_event_loop(self):
        log(self.peer_id, f'eventloop start')
        try:
            req: LocalRequest
            while True:
                req = await self._queue.get()
                if req.method == 'transport':
                    asyncio.create_task(self.create_p2p_transport(req))
                elif req.method == 'ping':
                    asyncio.create_task(self.ping_test(req))
                elif req.method == 'close':
                    break
                else:
                    asyncio.create_task(self.handle_http(req))
            log(self.peer_id, f'eventloop exit')
        except Exception as e:
            log(self.peer_id, f'eventloop error: {e}')

    async def do_request(self, request: LocalRequest, timeout: int = 60):
        request.future = asyncio.Future()
        self._queue.put_nowait(request)
        try:
            await asyncio.wait_for(request.future, timeout)
            return request.future.result()
        except Exception as e:
            log(self.peer_id, f'{request} error {e.__class__.__name__} {e}')
            return None

    def connected(self):
        return self.channel and self.channel.state == 'joined'

    async def start(self):
        self.peer_id = str(uuid.uuid1())
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


class HttpPeer:
    ''' http peer '''

    def __init__(self, use_options: bool = True):
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

    def start(self):
        pass

    def stop(self):
        pass

    def connected(self):
        return True

    async def do_request(self, request: LocalRequest, timeout: int = 60):
        if request.method == 'transport':
            netloc = request.uri
            await self.http_relay(netloc, request.reader, request.writer)
        return None

    async def http_relay(self, netloc: str, local_reader: asyncio.StreamReader, local_writer: asyncio.StreamWriter):
        host, port = netloc.split(':')
        if not host:
            raise RuntimeError('invalid netloc')
        if not port:
            port = '443'
        reader, writer = await asyncio.open_connection(host, port)
        asyncio.ensure_future(relay_reader_to_writer(local_reader, writer, 'local->remote'))
        await relay_reader_to_writer(reader, local_writer, 'remote->local')


class HttpServer:
    ''' http proxy '''

    def __init__(self, endpoint: ProxyPeer):
        self.logger = logging.getLogger('proxy')
        self.endpoint = endpoint

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            request_line = (await reader.readline()).decode()
            method, uri, _ = request_line.split()
        except Exception:
            writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
            safe_close(writer)
            return

        self.logger.info(f'received {method} {uri}')
        # client_peername = writer.get_extra_info('peername')
        # endpoint readiness check
        if method == 'GET' and uri == '/':
            if self.endpoint.connected():
                writer.write(b'HTTP/1.1 200 OK\r\n\r\n')
            else:
                writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
            safe_close(writer)
            return
        # custom testing
        if method == 'GET' and uri == '/ping':
            res = await self.endpoint.do_request(LocalRequest('ping'))
            writer.write(f'HTTP/1.1 200 {res}\r\n\r\n'.encode())
            safe_close(writer)
            return

        # https proxy
        if method != 'CONNECT':
            # http proxy, request line: GET http://host:port/path HTTP/1.1
            if not uri.startswith('http://'):
                writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
                safe_close(writer)
                return

        # request new dc
        if self.endpoint and self.endpoint.connected():
            req = LocalRequest(
                method.upper(),
                uri,
                self.endpoint.peer_id,
                reader,
                writer
            )
            await self.endpoint.do_request(req)
            self.logger.info(f'done {method} {uri}')
        else:
            self.logger.info(f'rejected {method} {uri}')
            writer.write(b'HTTP/1.1 500 Not ready\r\n\r\n')
            safe_close(writer)

    async def start(self, port: int = 1234):
        server = await asyncio.start_server(self.handle_request, port=port)
        addr = server.sockets[0].getsockname()
        self.logger.info(f'Local server running on {addr[0]}:{addr[1]}')

        await self.endpoint.start()
        async with server:
            await server.serve_forever()

    async def stop(self):
        if self.endpoint:
            await self.endpoint.stop()


async def main():
    process = None
    try:
        if '--server' in sys.argv:
            process = HttpServer(RealtimePeer())
            await process.start(port=8001)
        elif '--http' in sys.argv:
            process = HttpServer(HttpPeer())
            await process.start(port=2334)
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
