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

signal_room = os.environ.get('SIGNAL_ROOM', 'fd1e313f-cea1-408a-b1f2-13379d256876')
supabase_key = os.environ.get('SUPABASE_KEY', '')
supabase_url = os.environ.get('SUPABASE_URL', '')

DEFAULT_CONFIG = RTCConfiguration()
RELAY_BUFFER_SIZE = 4096
BOOTSTRAP_LABEL = 'bootstrap'
TRANSPORT_LABEL = 'transport'

logger = logging.getLogger('logger')


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

    type: str = None
    "The type of the event"

    content: Optional[str] = None
    "anything"

    tid: str = str(uuid.uuid1())
    "transaction id"

    future: Optional[asyncio.Future] = None

    def asdict(self) -> dict:
        return {
            'tid': self.tid,
            'type': self.type,
            'content': self.content,
        }

    def to_json(self) -> str:
        self_dict = self.asdict()
        none_removed = {k: v for k, v in self_dict.items() if v is not None}
        return json.dumps(none_removed)

    @classmethod
    def from_json(cls, json_str: str):
        self_dict = json.loads(json_str)
        return cls(**self_dict)


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


def log(sid: str, msg: str):
    logger.info(msg=f'{sid} {msg}')


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


async def safe_write(writer, data):
    ''' Safely write data to a StreamWriter '''
    try:
        if writer and len(data) and not writer.is_closing():
            writer.write(data)
            await writer.drain()
    except Exception:
        safe_close(writer)


async def safe_write_buffers(writer, buffers: list):
    ''' Safely write all buffers to a StreamWriter '''
    if not writer or writer.is_closing():
        return
    try:
        for b in buffers:
            writer.write(b)
        await writer.drain()
        buffers.clear()
    except Exception:
        safe_close(writer)


async def relay_reader_to_dc(reader: asyncio.StreamReader, pair: DataChannelPair, tid: str):
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
    log(pair.signal_sid, f'dc {pair.get_label()} relays done {tid}')


class RealtimePeer:
    ''' A peer that uses supabase realtime as signaling server '''

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._outgoing_requests: dict[str, ChannelCommand] = {}
        self.signal_sid: str = None
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

    ''' Get connected RTCPeerConnection '''
    def _get_connected_peer(self, tid: str):
        pc = self.peers.get(tid, None)
        if pc and pc.connectionState == 'connected':
            return pc
        else:
            return None

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

        self.peers[tid] = pc = RTCPeerConnection(DEFAULT_CONFIG)

        @pc.on('connectionstatechange')
        def on_connection_state():
            log(tid, f'{pc.connectionState}')
            if pc.connectionState == 'failed':
                asyncio.ensure_future(safe_close_peer(pc))

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

    async def _handle_incoming_request(self, req: ChannelCommand):
        if req.method == 'ping':
            self._send_channel_command(ChannelCommand(req.tid, body='pong'))
        elif req.method == 'connect':
            # TODO handle new peer request in random delay
            res = await self.handle_p2p_connect(req)
            self._send_channel_command(res)

    def _on_channel_command(self, msg):
        try:
            channelMessage = ChannelMessage.fromdict(msg['payload'])
            if self.signal_sid == channelMessage.id:
                return

            cmd: ChannelCommand = ChannelCommand.fromdict(channelMessage.content)
            if not cmd.tid:
                log(self.signal_sid, f'invalid command no tid {cmd}')
                return

            log(cmd.tid, f'recv command {cmd}')
            if cmd.tid in self._outgoing_requests:
                # get response from other peer
                req = self._outgoing_requests.pop(cmd.tid)
                if req and req.future and not req.future.done():
                    req.future.set_result(cmd)
                return

            # handle incoming request
            asyncio.ensure_future(self._handle_incoming_request(cmd))
        except Exception as e:
            log(self.signal_sid, f'handle incoming message error {e}')

    def _send_channel_command(self, req: ChannelCommand, timeout: int = 30):
        if req is None:
            return
        if not self.connected():
            req.future.set_exception(Exception('channel not ready'))
            return

        msg = ChannelMessage(self.signal_sid, 'command', req.asdict(), str(time.time()))
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
            log(self.signal_sid, f'channel request error {e.__class__.__name__} {e}')
            if req.future.exception() is None:
                req.future.set_exception(Exception('timeout'))
            return None
        finally:
            self._outgoing_requests.pop(req.tid, None)

    async def _create_p2p_transport(self, req: LocalRequest):
        tid = req.tid

        def on_open():
            def on_new_dc_open():
                log(tid, f'dc {new_dc.label} opened')
                req.future.set_result(DataChannelPair.new_p2p(tid, new_dc))

            new_dc = peer.createDataChannel(req.content)
            new_dc.once('open', on_new_dc_open)

        def on_close():
            if not req.future.done():
                req.future.set_exception(Exception('peer closed'))

        peer: RTCPeerConnection = self._get_connected_peer(tid)
        if peer:
            on_open()
            return

        if self.peers.get(tid, None):
            req.future.set_exception(Exception('peer connecting'))
            return

        # create new peer connection
        peer = await self.create_peer(tid, opened=on_open, closed=on_close)
        res = await self.channel_request(ChannelCommand(tid=tid, method='connect', body={
            'offer': peer.localDescription.sdp, 'ice': []
        }))

        if not res:
            req.future.set_result(None)
            await safe_close_peer(peer)
            return

        if res.error:
            req.future.set_exception(Exception(res.error))
            await safe_close_peer(peer)
            return

        if not res.body or 'answer' not in res.body:
            req.future.set_exception(Exception('invalid response'))
            await safe_close_peer(peer)
            return

        await peer.setRemoteDescription(RTCSessionDescription(res.body['answer'], 'answer'))

    async def _ping_test(self, req: LocalRequest):
        res = await self.channel_request(ChannelCommand(method='ping'), 10)
        req.future.set_result(res.body if res else 'no response')
        log(self.signal_sid, f'ping test result {res}')

    async def _event_loop(self):
        log(self.signal_sid, f'eventloop start')
        try:
            req: LocalRequest
            while True:
                req = await self._queue.get()
                if req.type == 'transport':
                    asyncio.create_task(self._create_p2p_transport(req))
                elif req.type == 'ping':
                    asyncio.create_task(self._ping_test(req))
                elif req.type == 'close':
                    break
            log(self.signal_sid, f'eventloop exit')
        except Exception as e:
            log(self.signal_sid, f'loop error: {e}')

    async def do_request(self, request: LocalRequest):
        request.future = asyncio.Future()
        self._queue.put_nowait(request)
        try:
            await request.future
            res = request.future.result()
            return res
        except Exception as e:
            log(self.signal_sid, f'{request.type} request error {e}')
            return None

    def connected(self):
        return self.channel and self.channel.state == 'joined'

    async def start(self):
        self.signal_sid = str(uuid.uuid1())
        self.client = await acreate_client(supabase_url, supabase_key)
        self.channel = self.client.realtime.channel(f'room:{signal_room}:messages')
        self.channel.on_broadcast('command', self._on_channel_command)
        await self.channel.subscribe()
        asyncio.create_task(self._event_loop())

    async def stop(self):
        self._queue.put_nowait(LocalRequest('close'))
        if (self.channel):
            self.client.remove_channel(self.channel)
            self.channel = None
        if len(self.peers):
            asyncio.gather(*[peer.close() for peer in self.peers.values()])

class HttpServer:
    ''' http proxy '''

    def __init__(self, endpoint: RealtimePeer):
        self.logger = logging.getLogger('proxy')
        self.endpoint = endpoint
        self.server_id = str(uuid.uuid1())

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            request_line = (await reader.readline()).decode()
            method, netloc, _ = request_line.split()
        except Exception:
            writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
            safe_close(writer)
            return

        self.logger.info(f'received {method} {netloc}')
        # client_peername = writer.get_extra_info('peername')
        # https proxy only
        if method != 'CONNECT':
            url = urllib.parse.urlparse(netloc)
            netloc = f'{url.netloc}:{url.port or 80}'

            if method == 'PING':
                await self.endpoint.do_request(LocalRequest('ping'))
                writer.write(b'HTTP/1.1 200 OK\r\n\r\n')
                safe_close(writer)
                return

        # read all headers
        all_headers = await reader.readuntil(b'\r\n\r\n')

        def reject():
            self.logger.info(f'rejected {netloc}')
            writer.write(b'HTTP/1.1 500 Not ready\r\n\r\n')
            safe_close(writer)

        async def handle_dc_open(transport: DataChannelPair, tid: str):
            if method == 'CONNECT':
                http200 = b'HTTP/1.1 200 Connection established\r\n\r\n'
                asyncio.ensure_future(safe_write(writer, http200))
            elif tid:
                transport.transfer(tid, request_line.encode())
                transport.transfer(tid, all_headers)
            else:
                transport.send(request_line.encode())
                transport.send(all_headers)
            await relay_reader_to_dc(reader, transport, tid)

        # request new dc
        if self.endpoint and self.endpoint.connected():
            tid = self.server_id
            transport: DataChannelPair = await self.endpoint.do_request(LocalRequest('transport', netloc, tid))
            if not transport or not transport.is_ready():
                self.logger.info(f'transport {transport} not ready {netloc}')
                reject()
                return

            @transport.receiver.on('message')
            def on_message(msg):
                if isinstance(msg, str):
                    return
                asyncio.ensure_future(safe_write(writer, msg))

            await handle_dc_open(transport, None)
        else:
            reject()

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
    asyncio.run(main())
