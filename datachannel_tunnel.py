from dataclasses import dataclass, asdict
from typing import Optional
import urllib.parse
import logging
import json
import time
import os
import pickle

import asyncio
import httpx
from aiortc import RTCDataChannel, RTCPeerConnection, RTCSessionDescription, RTCConfiguration

signal_room = os.environ.get('SIGNAL_ROOM', 'defaultsignal')
signal_base_url = os.environ.get('SIGNAL_BASE_URL', 'http://localhost:8000')
session_url = f'https://cfstream.lichon.cc/api/sessions'
signal_url = f'{signal_base_url}/api/signals/{signal_room}'

DEFAULT_CONFIG = RTCConfiguration()
RELAY_BUFFER_SIZE = 4096
TRANSPORT_LABEL = 'transfer'
RPC_LABLE = 'channel'

logger = logging.getLogger('logger')


def log(sid: str, msg: str):
    logger.info(msg=f"{sid} {msg}")


def safe_close(writer: asyncio.StreamWriter):
    try:
        if writer and not writer.is_closing():
            writer.close()
    except Exception:
        pass


async def safe_write(writer, data):
    try:
        if writer and len(data) and not writer.is_closing():
            writer.write(data)
            await writer.drain()
    except Exception:
        safe_close(writer)


async def create_cf_peer(pc: RTCPeerConnection, on_dc_open=None, on_dc_close=None):
    if pc:
        try:
            await pc.close()
        except Exception:
            pass

    pc = RTCPeerConnection(DEFAULT_CONFIG)
    dc = pc.createDataChannel('bootstrap')
    sid = None

    @dc.on("open")
    def on_open():
        if on_dc_open:
            on_dc_open(dc, sid)

    @dc.on("close")
    def on_close():
        if on_dc_close:
            on_dc_close(dc, sid)

    @pc.on("connectionstatechange")
    def on_connection_state():
        log(sid, f"{pc.connectionState}")

    await pc.setLocalDescription(await pc.createOffer())
    async with httpx.AsyncClient() as client:
        resp = await client.post(session_url, content=pc.localDescription.sdp)
        sid = resp.headers['Location'].split('/')[-1]
        await pc.setRemoteDescription(RTCSessionDescription(resp.text, 'answer'))
    return pc


async def request_cf_datachannel(pc: RTCPeerConnection, local: str, remote: str = None, label: str = None):
    channel_id = None
    channel_label = label or RPC_LABLE
    config = {
        'sessionId': remote or local,
        'location': 'remote' if remote else 'local',
        'dataChannelName': channel_label,
    }
    async with httpx.AsyncClient() as client:
        resp = await client.patch(f'{session_url}/{local}', json={'dataChannels': [config]}, timeout=30)
        resp_json = resp.json()
        dcs = resp_json.get('dataChannels')
        if dcs and isinstance(dcs, list) and len(dcs) > 0:
            channel_id = dcs[0].get('id')
        if channel_id is None:
            log('', f'dc request res {dcs}')

    return pc.createDataChannel(channel_label, negotiated=True, id=channel_id) if channel_id else None


class TransferData:
    """ relay data with tid send/receive through one channel """
    tid: int
    data: bytes


@dataclass
class RpcEvent:
    type: str
    "The type of the event"

    content: Optional[str] = None
    "anything"

    tid: float = time.time()
    "transaction id"

    future: Optional[asyncio.Future] = None

    def to_json(self) -> str:
        saved_future = self.future
        self.future = None
        self_dict = asdict(self)
        self.future = saved_future
        none_removed = {k: v for k, v in self_dict.items() if v is not None}
        return json.dumps(none_removed)

    @classmethod
    def from_json(cls, json_str: str):
        self_dict = json.loads(json_str)
        return cls(**self_dict)


class DataChannelPair:
    """ A pair of data channels """

    def __init__(self,
                 signal_sid: str,
                 local_sid: str,
                 remote_sid: str,
                 sender: RTCDataChannel,
                 receiver: RTCDataChannel):
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
        if self.is_ready():
            self.sender.send(data)

    def transfer(self, tid, data):
        if self.is_ready():
            transfer_data = TransferData()
            transfer_data.tid = tid
            transfer_data.data = data
            dumps = pickle.dumps(transfer_data)
            self.sender.send(dumps)


async def relay_reader_to_dc(reader: asyncio.StreamReader, pair: DataChannelPair, tid: str):
    while not reader.at_eof():
        if (pair.sender.bufferedAmount > RELAY_BUFFER_SIZE * 100):
            await asyncio.sleep(1)
            continue
        data = await reader.read(RELAY_BUFFER_SIZE)
        if not data:
            break
        pair.transfer(tid, data)
    log(pair.signal_sid, f'dc {pair.get_label()} relays done {tid}')


class DcRelayServer:
    """ data channel relay server """

    def __init__(self):
        self.signal_peer: RTCPeerConnection = None
        self.rpc_peer: RTCPeerConnection = None
        self.data_peer: RTCPeerConnection = None

    async def _relay_handler(self, transport: DataChannelPair, request: RpcEvent, callback):
        if not transport:
            callback(RuntimeError('transfer not ready'))
            return
        try:
            tid = request.tid
            host, port = request.content.split(':')
            if not host or not port:
                raise RuntimeError('invalid label')

            @transport.receiver.on('message')
            def on_message(msg):
                if isinstance(msg, str):
                    return
                pickled = pickle.loads(msg)
                if pickled.tid == tid:
                    asyncio.ensure_future(safe_write(writer, pickled.data))

            reader, writer = await asyncio.open_connection(host, port)
            log(transport.signal_sid, f'dc {tid} connected to {host}:{port}')
            asyncio.ensure_future(relay_reader_to_dc(reader, transport, tid))
            callback(None)
        except Exception as e:
            callback(e)

    async def _create_transport(self, rpc_channel: DataChannelPair, pair_res, callback):
        try:
            label = TRANSPORT_LABEL
            receiver_req = request_cf_datachannel(self.rpc_peer, rpc_channel.local_sid, rpc_channel.remote_sid, label)
            sender_req = request_cf_datachannel(self.rpc_peer, rpc_channel.local_sid, None, label)
            receiver, sender = await asyncio.gather(receiver_req, sender_req)

            pair = rpc_channel.create_pair(sender, receiver)
            if not pair.is_valid():
                raise RuntimeError('relay pair init failed')
            pair_res[0] = pair
            callback(None)
        except Exception as e:
            callback(e)

    async def _rpc_handler(self, rpc_channel: DataChannelPair):
        signal_sid = rpc_channel.signal_sid
        log(signal_sid, f'start rpc channel {rpc_channel.get_sid_pair()} (local, remote)')
        if rpc_channel.receiver is None:
            log(signal_sid, f'receiver is none')
            return

        label = rpc_channel.get_label()
        transfer_pair_holder = [None]

        @rpc_channel.receiver.on("message")
        def on_message(msg):
            if not isinstance(msg, str):
                return
            log(signal_sid, f'dc {label} <<< {msg}')
            request = RpcEvent.from_json(msg)

            def send_response(e: Exception = None):
                response = RpcEvent(type='ok' if e is None else 'error', tid=request.tid)
                response.content = f'{e}' if e else None
                resp_json = response.to_json()
                log(signal_sid, f'dc {label} >>> {resp_json}')
                rpc_channel.send(resp_json)

            if request.type == 'relay':
                asyncio.create_task(self._relay_handler(transfer_pair_holder[0], request, send_response))
            elif request.type == 'transfer':
                asyncio.create_task(self._create_transport(rpc_channel, transfer_pair_holder, send_response))
            elif request.type == 'ping':
                send_response()
                # update signal connection
                asyncio.create_task(self._start_signal_peer(rpc_channel.sender, rpc_channel.local_sid))

    async def _check_client_offer(self, signal_sid, channel_sid: str, sender: RTCDataChannel):
        log(signal_sid, "checking client offer")
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(signal_url)
                signal = resp.json()
                offer_sid = signal.get('offer')
                sid = signal.get('sid')
                answer_sid = signal.get('answer')
                if offer_sid and sid == signal_sid and channel_sid == answer_sid:
                    # client ready start rpc handler
                    receiver = await request_cf_datachannel(self.rpc_peer, channel_sid, offer_sid)
                    await self._rpc_handler(DataChannelPair(sid, channel_sid, offer_sid, sender, receiver))
                else:
                    log(signal_sid, f'offer not match channel_sid {answer_sid} {signal}')
        except Exception as e:
            log(signal_sid, f"retry client offer in 10s {e}")
            def retry(): return asyncio.ensure_future(self._check_client_offer(signal_sid, channel_sid, sender))
            asyncio.get_event_loop().call_later(10, retry)

    def _update_server_signal(self, signal_sid, channel_sid):
        async def async_update():
            async with httpx.AsyncClient() as client:
                await client.post(signal_url, json={'sid': signal_sid, 'answer': channel_sid})
            log(signal_sid, f"server channel updated {channel_sid}")
        asyncio.create_task(async_update())

    async def _start_signal_peer(self, sender: RTCDataChannel, channel_sid: str):
        def on_open(_, sid):
            self._update_server_signal(sid, channel_sid)

        # kicked by client, start to check client offer
        def on_close(_, sid):
            asyncio.ensure_future(self._check_client_offer(sid, channel_sid, sender))

        self.signal_peer = await create_cf_peer(self.signal_peer, on_open, on_close)

    def connected(self):
        return False

    async def start(self):
        def restart_server():
            return asyncio.create_task(self.start())

        async def new_signal(rpc_sid):
            sender = await request_cf_datachannel(self.rpc_peer, rpc_sid)

            @sender.on('close')
            def on_close():
                self._update_server_signal('', '')
                # TODO check other threads done
                asyncio.get_event_loop().call_later(10, restart_server)

            await self._start_signal_peer(sender, rpc_sid)

        def on_open(dc: RTCDataChannel, sid):
            dc.close()

        def on_close(dc: RTCDataChannel, sid):
            asyncio.ensure_future(new_signal(sid))

        self.rpc_peer = await create_cf_peer(self.rpc_peer, on_open, on_close)

    async def stop(self):
        if self.signal_peer:
            await self.signal_peer.close()
        if self.rpc_peer:
            await self.rpc_peer.close()
        if self.data_peer:
            await self.data_peer.close()


class DcRelayClient:
    """ data channel relay client """

    CLIENT_DELAY: int = 30
    """ client delay before checking signal """

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self.signal_sid: str = None
        self.rpc_peer: RTCPeerConnection = None
        self.data_peer: RTCPeerConnection = None

    def _restart(self):
        asyncio.ensure_future(self.start())

    async def _send_rpc(self, rpc_channel: DataChannelPair, request: RpcEvent, timeout=None) -> RpcEvent:
        if not request:
            return None

        if not rpc_channel.is_ready():
            log(self.signal_sid, f"dc channel is not ready")
            raise Exception("dc channel closed")

        sender, receiver = rpc_channel.get_pair()
        rpc_future = asyncio.Future()

        def on_message(msg):
            if not isinstance(msg, str):
                return
            if rpc_future.cancelled():
                return
            response = RpcEvent.from_json(msg)
            if response.tid == request.tid:
                rpc_future.set_result(response)

        receiver.add_listener('message', on_message)
        try:
            log(self.signal_sid, f"dc {sender.label} >>> {request.to_json()}")
            sender.send(request.to_json())
            await asyncio.wait_for(rpc_future, timeout or 30)
        except asyncio.TimeoutError:
            pass
        finally:
            receiver.remove_listener('message', on_message)

        return None if rpc_future.cancelled() else rpc_future.result()

    async def _ping_until_success(self, rpc_channel: DataChannelPair):
        ping = RpcEvent('ping')
        res = None
        while not res:
            await asyncio.sleep(1)
            res = await self._send_rpc(rpc_channel, ping, 1)
        return res

    async def _update_client_signal(self, local_sid, remote_sid):
        async with httpx.AsyncClient() as client:
            # send kick to signal
            log(self.signal_sid, f'kick signal')
            localSdp = self.rpc_peer.localDescription.sdp
            kick_res = await client.patch(f'{session_url}/{self.signal_sid}', content=localSdp, timeout=30)
            # kick failed or timeout
            if kick_res.is_error:
                log(self.signal_sid, f'kick failed {kick_res.content}')
                return False

            # update client offer to signal
            await client.post(
                signal_url,
                json={'sid': self.signal_sid, 'offer': local_sid, 'answer': remote_sid},
                timeout=30
            )
        return True

    async def _create_transport(self, rpc_channel: DataChannelPair) -> DataChannelPair:
        local_sid, remote_sid = rpc_channel.get_sid_pair()
        # prepare local sender first, so server can connect to it
        sender = await request_cf_datachannel(self.rpc_peer, local_sid, None, TRANSPORT_LABEL)
        if not sender:
            return None

        # request remote pair
        resp = await self._send_rpc(rpc_channel, RpcEvent(TRANSPORT_LABEL))
        if not resp or resp.type != 'ok':
            sender.close()
            return None

        # prepare local receiver, after rpc done
        receiver = await request_cf_datachannel(self.rpc_peer, local_sid, remote_sid, TRANSPORT_LABEL)
        return rpc_channel.create_pair(sender, receiver)

    async def _relay_request(self, rpc_channel: DataChannelPair, transport: DataChannelPair, req: RpcEvent):
        # request remote pair
        resp = await self._send_rpc(rpc_channel, req)
        if not resp or resp.type != 'ok':
            req.future.set_result(None)
            return
        req.future.set_result(transport)

    async def _p2p_request(self, channel_pair: DataChannelPair, transfer: DataChannelPair, req: RpcEvent):
        pass

    async def _event_loop(self, local_sid, remote_sid):
        try:
            receiver = await request_cf_datachannel(self.rpc_peer, local_sid, remote_sid)
            if receiver is None:
                log(self.signal_sid, f'failed to create channel receiver')
                raise

            @receiver.on('message')
            def on_message(msg):
                if not isinstance(msg, str):
                    return
                log(self.signal_sid, f'dc {receiver.label} <<< {msg}')

            sender = await request_cf_datachannel(self.rpc_peer, local_sid)
            if sender is None:
                log(self.signal_sid, f'failed to create channel sender')
                raise

            if not await self._update_client_signal(local_sid, remote_sid):
                raise

            rpc_channel = DataChannelPair(self.signal_sid, local_sid, remote_sid, sender, receiver)
            await asyncio.sleep(self.CLIENT_DELAY)
            await self._ping_until_success(rpc_channel)
            transport = await self._create_transport(rpc_channel)
            if transport is None:
                log(self.signal_sid, f'failed to create transport')
                raise

            log(self.signal_sid, f'client eventloop start')

            event: RpcEvent
            while rpc_channel.is_ready():
                event = await self._queue.get()
                if event.type == 'relay':
                    asyncio.create_task(self._relay_request(rpc_channel, transport, event))
                elif event.type == 'p2p':
                    asyncio.create_task(self._p2p_request(rpc_channel, transport, event))

            log(self.signal_sid, f'client eventloop exit')
        except Exception as e:
            log(self.signal_sid, f'client loop error: {e}')

        asyncio.get_event_loop().call_later(30, self._restart)

    async def enqueue(self, request: RpcEvent):
        request.future = asyncio.Future()
        self._queue.put_nowait(request)
        await request.future
        return request.future.result()

    def connected(self):
        return self.rpc_peer and self.rpc_peer.connectionState == 'connected'

    async def start(self):
        async with httpx.AsyncClient() as client:
            resp = await client.get(signal_url)
            if not resp.is_success:
                log(self.signal_sid, 'signal room may be invalid')
                asyncio.get_event_loop().call_later(30, self._restart)
                return

            signal = resp.json()
            sid = signal.get('sid')
            remote_sid = signal.get('answer')
            if not sid or not remote_sid:
                log(sid, 'signal is not ready')
                asyncio.get_event_loop().call_later(30, self._restart)
                return

            self.signal_sid = sid

            def on_open(dc, _):
                # close bootstrap dc to release dc stream id
                dc.close()

            def on_close(dc, local_sid):
                # bootstrap dc id released
                asyncio.create_task(self._event_loop(local_sid, remote_sid))

            self.rpc_peer = await create_cf_peer(self.rpc_peer, on_open, on_close)

    async def stop(self):
        if self.rpc_peer:
            await self.rpc_peer.close()
        if self.data_peer:
            await self.data_peer.close()


class HttpServer:
    """ http proxy """

    def __init__(self, relay_endpoint: DcRelayClient | DcRelayServer):
        self.logger = logging.getLogger('proxy')
        self.endpoint = relay_endpoint

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            request_line = (await reader.readline()).decode()
            method, netloc, _ = request_line.split()
        except Exception:
            writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
            safe_close(writer)
            return

        self.logger.info(f"received {method} {netloc}")
        is_connect = method == 'CONNECT'
        client_peername = writer.get_extra_info('peername')
        # https proxy only
        if not is_connect:
            url = urllib.parse.urlparse(netloc)
            netloc = f'{url.netloc}:{url.port or 80}'

        # read all headers
        all_headers = await reader.readuntil(b'\r\n\r\n')

        def reject():
            self.logger.info(f'rejected {netloc}')
            writer.write(b"HTTP/1.1 500 Not ready\r\n\r\n")
            safe_close(writer)

        async def handle_dc_open(transport: DataChannelPair, tid: str):
            if is_connect:
                http200 = b"HTTP/1.1 200 Connection established\r\n\r\n"
                asyncio.ensure_future(safe_write(writer, http200))
            else:
                transport.transfer(tid, request_line.encode())
                transport.transfer(tid, all_headers)
            await relay_reader_to_dc(reader, transport, tid)

        # request new dc
        if self.endpoint and self.endpoint.connected():
            # localhost test,
            tid = client_peername[1]
            # get dc pair
            transport: DataChannelPair = await self.endpoint.enqueue(RpcEvent('relay', netloc, tid=tid))
            if not transport or not transport.is_ready():
                reject()
                return

            @transport.receiver.on("message")
            def on_message(msg):
                if isinstance(msg, str):
                    return
                pickled = pickle.loads(msg)
                if tid == pickled.tid:
                    asyncio.ensure_future(safe_write(writer, pickled.data))

            await handle_dc_open(transport, tid)
        else:
            reject()

    async def start(self, port: int | None):
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
        if '--client' in sys.argv:
            process = HttpServer(DcRelayClient())
            await process.start(port=2234)
        else:
            process = HttpServer(DcRelayServer())
            await process.start(port=8001)
    except KeyboardInterrupt:
        pass
    finally:
        await process.stop()

if __name__ == '__main__':
    import sys
    from aioice import ice
    ice.CONSENT_INTERVAL = 2
    ice.CONSENT_FAILURES = 5
    DcRelayClient.CLIENT_DELAY = ice.CONSENT_FAILURES * ice.CONSENT_INTERVAL
    # Setup logging configuration
    debug = '--debug' in sys.argv
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    asyncio.run(main())
