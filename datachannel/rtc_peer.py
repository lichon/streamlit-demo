from dataclasses import dataclass, asdict
from typing import Optional
import urllib.parse
import json
import time
import os
import pickle
import uuid
import asyncio

from supabase import acreate_client, AsyncClient
from realtime import AsyncRealtimeChannel
from aiortc import RTCDataChannel, RTCPeerConnection, RTCSessionDescription
from proxy_peer import ProxyPeer, LocalRequest, log, safe_close, safe_write

RELAY_BUFFER_SIZE = 4096
BOOTSTRAP_LABEL = 'bootstrap'
TRANSPORT_LABEL = 'transport'

peer_name = os.environ.get('HOSTNAME', 'Realtime Peer')
signal_room = os.environ.get('SIGNAL_ROOM', 'default')
supabase_key = os.environ.get('SUPABASE_KEY', '')
supabase_url = os.environ.get('SUPABASE_URL', '')


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


async def safe_close_peer(pc: RTCPeerConnection):
    try:
        if pc:
            await pc.close()
    except Exception:
        pass


class RtcPeer(ProxyPeer):
    ''' A peer that uses supabase realtime as signaling server '''

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._outgoing_requests: dict[str, ChannelCommand] = {}
        self.peer_id = str(uuid.uuid1())
        self.client: AsyncClient = None
        self.channel: AsyncRealtimeChannel = None
        self.peers: dict[str, RTCPeerConnection] = {}
        self._peers_lock = asyncio.Lock()

    async def relay_reader_to_dc(self, reader: asyncio.StreamReader, pair: DataChannelPair, tid: str = None):
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
            await self.relay_reader_to_dc(reader, transport)
        except Exception:
            pass
        finally:
            transport.close()
            safe_close(writer)

    ''' Create a new RTCPeerConnection with a bootstrap DataChannel '''

    async def create_peer(self, peer_id: str, opened=None, closed=None):
        await safe_close_peer(self.peers.get(peer_id, None))
        log(peer_id, f'create new peer')
        self.peers[peer_id] = pc = RTCPeerConnection()

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
            req.reject()
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

            await self.relay_reader_to_dc(req.reader, transport)
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
            log(trace_tag, f'dc {req.method} error {e.__class__.__name__} {e}')
            return None

    async def init_channel(self):
        self.client = await acreate_client(supabase_url, supabase_key)
        self.channel = self.client.channel(f'room:{signal_room}:messages')
        self.channel.on_broadcast('command', self._recv_channel_command)
        await self.channel.subscribe()
        await self.channel.track({
            'id': self.peer_id,
            'name': peer_name,
        })

    async def recover(self):
        if self.client and not self.client.get_channels():
            await self.init_channel()

    def connected(self):
        return self.channel and self.channel.is_joined

    async def start(self):
        if self.client:
            return
        await self.init_channel()
        asyncio.create_task(self.start_event_loop())

    async def stop(self):
        if self.client:
            await self.client.remove_all_channels()
        self._queue.put_nowait(LocalRequest('close'))
        await asyncio.gather(*[peer.close() for peer in self.peers.values()])
