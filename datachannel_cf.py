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

default_config = RTCConfiguration(iceServers=[])
session_url = f'https://cfstream.lichon.cc/api/sessions'
signal_url = f'{signal_base_url}/api/signals/{signal_room}'

signal_pc: RTCPeerConnection = None
bootstrap_pc: RTCPeerConnection = None
client_queue: asyncio.Queue = asyncio.Queue()

relay_buffer_size = 4096
client_delay = 30

proxy_logger = logging.getLogger('proxy ')
logger = logging.getLogger('logger')


class DataChannelPair:
    """ A pair of data channels """

    sender: RTCDataChannel
    receiver: RTCDataChannel

    local_sid: str
    remote_sid: str
    signal_sid: str

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


class TransferData:
    tid: int
    data: bytes


@dataclass
class ClientEvent:
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


async def safe_write_close_dc(writer: asyncio.StreamWriter, data, dc: RTCDataChannel):
    if not writer or writer.is_closing():
        dc.close()
        return
    try:
        writer.write(data)
        await writer.drain()
    except Exception as e:
        proxy_logger.warning(f"dc {dc.id} write to stream failed: {e}")
        safe_close(writer)
        dc.close()


async def relay_reader_to_dc(reader: asyncio.StreamReader, pair: DataChannelPair, tid: str):
    while not reader.at_eof():
        if (pair.sender.bufferedAmount > relay_buffer_size * 100):
            await asyncio.sleep(1)
            continue
        data = await reader.read(relay_buffer_size)
        if not data:
            break
        pair.transfer(tid, data)
    log(pair.signal_sid, f'dc {tid} relays done')


async def server_relay_handler(channel: DataChannelPair, request: ClientEvent, callback):
    if not channel:
        callback(RuntimeError('transfer not ready'))
        return
    try:
        tid = request.tid
        host, port = request.content.split(':')
        if not host or not port:
            raise RuntimeError('invalid label')

        @channel.receiver.on('message')
        def on_message(msg):
            if isinstance(msg, str):
                return
            pickled = pickle.loads(msg)
            if pickled.tid == tid:
                asyncio.ensure_future(safe_write(writer, pickled.data))

        reader, writer = await asyncio.open_connection(host, port)
        log(channel.signal_sid, f'dc {tid} connected to {host}:{port}')
        asyncio.ensure_future(relay_reader_to_dc(reader, channel, tid))
        callback(None)
    except Exception as e:
        callback(e)


async def server_transfer_handler(channel: DataChannelPair, pair_res, callback):
    global bootstrap_pc
    try:
        label = 'transfer'
        receiver_req = request_cf_datachannel(bootstrap_pc, channel.local_sid, channel.remote_sid, label)
        sender_req = request_cf_datachannel(bootstrap_pc, channel.local_sid, None, label)
        receiver, sender = await asyncio.gather(receiver_req, sender_req)

        pair = channel.create_pair(sender, receiver)
        if not pair.is_valid():
            raise RuntimeError('relay pair init failed')
        pair_res[0] = pair
        callback(None)
    except Exception as e:
        callback(e)


async def server_rpc_handler(rpc_channel: DataChannelPair):
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
        request = ClientEvent.from_json(msg)

        def send_response(e: Exception = None):
            response = ClientEvent(type='ok' if e is None else 'error', tid=request.tid)
            response.content = f'{e}' if e else None
            resp_json = response.to_json()
            log(signal_sid, f'dc {label} >>> {resp_json}')
            rpc_channel.send(resp_json)

        if request.type == 'relay':
            asyncio.create_task(server_relay_handler(transfer_pair_holder[0], request, send_response))
        elif request.type == 'transfer':
            asyncio.create_task(server_transfer_handler(rpc_channel, transfer_pair_holder, send_response))
        elif request.type == 'ping':
            send_response()
            # update signal connection
            asyncio.create_task(start_signal_peer(rpc_channel.sender, rpc_channel.local_sid))


async def check_client_offer(signal_sid: str, channel_sid: str, sender: RTCDataChannel):
    global bootstrap_pc

    log(signal_sid, "checking client offer")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(signal_url)
            signal = resp.json()
            offer_sid = signal.get('offer')
            sid = signal.get('sid')
            answer_sid = signal.get('answer')
            if offer_sid and sid == signal_sid and channel_sid == answer_sid:
                receiver = await request_cf_datachannel(bootstrap_pc, channel_sid, offer_sid)
                await server_rpc_handler(DataChannelPair(sid, channel_sid, offer_sid, sender, receiver))
            else:
                log(signal_sid, f'offer not match channel_sid {answer_sid} {signal}')
    except Exception as e:
        log(signal_sid, f"retry client offer in 10s {e}")
        def retry(): return asyncio.ensure_future(check_client_offer(signal_sid, channel_sid, sender))
        asyncio.get_event_loop().call_later(10, retry)


async def create_cf_peer(pc: RTCPeerConnection, on_dc_open=None, on_dc_close=None):
    if pc:
        try:
            await pc.close()
        except Exception:
            pass

    pc = RTCPeerConnection(default_config)
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
    channel_label = label or 'rpc'
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


def update_server_signal(signal_sid, channel_sid):
    async def async_update():
        async with httpx.AsyncClient() as client:
            await client.post(signal_url, json={'sid': signal_sid, 'answer': channel_sid})
        log(signal_sid, f"server channel updated {channel_sid}")
    return asyncio.create_task(async_update())


async def start_signal_peer(server_sender: RTCDataChannel, channel_sid: str):
    global signal_pc

    def on_open(_, sid):
        update_server_signal(sid, channel_sid)

    # kicked by client, start to check client offer
    def on_close(_, sid):
        asyncio.ensure_future(check_client_offer(sid, channel_sid, server_sender))

    signal_pc = await create_cf_peer(signal_pc, on_open, on_close)


async def run_server():
    global bootstrap_pc

    def restart_server():
        return asyncio.create_task(run_server())

    async def create_channel(channel_sid):
        channel_sender = await request_cf_datachannel(bootstrap_pc, channel_sid)

        @channel_sender.on('close')
        def on_close():
            update_server_signal('', '')
            # TODO check other threads done
            asyncio.get_event_loop().call_later(10, restart_server)

        await start_signal_peer(channel_sender, channel_sid)

    def on_open(dc, sid):
        dc.close()

    def on_close(dc, sid):
        asyncio.ensure_future(create_channel(sid))

    bootstrap_pc = await create_cf_peer(bootstrap_pc, on_open, on_close)


async def client_relay_handler(channel_pair: DataChannelPair, transfer: DataChannelPair, req: ClientEvent):
    # request remote pair
    resp = await client_send_rpc(channel_pair, req)
    if not resp or resp.type != 'ok':
        req.future.set_result(None)
        return
    # req.future.set_result(channel_pair)
    req.future.set_result(transfer)


async def client_p2p_handler(channel_pair: DataChannelPair, transfer: DataChannelPair, req: ClientEvent):
    pass


async def start_client_eventloop(pc, sid, local_sid, remote_sid):
    global client_queue
    try:
        receiver = await request_cf_datachannel(pc, local_sid, remote_sid)
        if receiver is None:
            log(sid, f'failed to create channel receiver')
            raise

        @receiver.on('message')
        def on_message(msg):
            if not isinstance(msg, str):
                return
            log(sid, f'dc {receiver.label} <<< {msg}')

        sender = await request_cf_datachannel(pc, local_sid)
        if sender is None:
            log(sid, f'failed to create channel sender')
            raise

        if not await update_client_signal(sid, local_sid, remote_sid):
            raise

        channel_pair = DataChannelPair(sid, local_sid, remote_sid, sender, receiver)
        await asyncio.sleep(client_delay)

        await ping_test(channel_pair)

        transfer_pair = await create_transfer_channel(channel_pair)

        log(sid, f'client eventloop start')

        event: ClientEvent
        while channel_pair.is_ready():
            event = await client_queue.get()
            if event.type == 'relay':
                asyncio.create_task(client_relay_handler(channel_pair, transfer_pair, event))
            elif event.type == 'p2p':
                asyncio.create_task(client_p2p_handler(channel_pair, transfer_pair, event))

        log(sid, f'client eventloop exit')
    except Exception as e:
        log(sid, f'client loop error: {e}')

    asyncio.get_event_loop().call_later(30, restart_client)


async def create_transfer_channel(channel_pair: DataChannelPair) -> DataChannelPair:
    global bootstrap_pc

    label = "transfer"
    local_sid, remote_sid = channel_pair.get_sid_pair()
    # prepare local sender first, so server can connect to it
    sender = await request_cf_datachannel(bootstrap_pc, local_sid, None, label)
    if not sender:
        return None

    # request remote pair
    resp = await client_send_rpc(channel_pair, ClientEvent(label))
    if not resp or resp.type != 'ok':
        sender.close()
        return None

    # prepare local receiver, after rpc done
    receiver = await request_cf_datachannel(bootstrap_pc, local_sid, remote_sid, label)
    return channel_pair.create_pair(sender, receiver)


async def update_client_signal(signal_sid: str, local_sid: str, remote_sid: str) -> bool:
    global bootstrap_pc

    async with httpx.AsyncClient() as client:
        # send kick to signal
        log(signal_sid, f'kick signal')
        localSdp = bootstrap_pc.localDescription.sdp
        kick_res = await client.patch(f'{session_url}/{signal_sid}', content=localSdp, timeout=30)
        # kick failed or timeout
        if kick_res.is_error:
            log(signal_sid, f'kick failed {kick_res.content}')
            return False

        # update client offer to signal
        await client.post(
            signal_url,
            json={'sid': signal_sid, 'offer': local_sid, 'answer': remote_sid},
            timeout=30
        )
    return True


def restart_client():
    return asyncio.ensure_future(start_client())


async def start_client():
    global bootstrap_pc

    signal_sid = None

    async with httpx.AsyncClient() as client:
        resp = await client.get(signal_url)
        if not resp.is_success:
            log(signal_sid, 'signal room may be invalid')
            asyncio.get_event_loop().call_later(30, restart_client)
            return

        signal = resp.json()
        signal_sid = signal.get('sid')
        remote_sid = signal.get('answer')
        if not signal_sid or not remote_sid:
            log(signal_sid, 'signal is not ready')
            asyncio.get_event_loop().call_later(30, restart_client)
            return

        def on_open(dc, _):
            # close bootstrap dc to release dc stream id
            dc.close()

        def on_close(dc, local_sid):
            # bootstrap dc id released
            asyncio.create_task(start_client_eventloop(bootstrap_pc, signal_sid, local_sid, remote_sid))

        bootstrap_pc = await create_cf_peer(bootstrap_pc, on_open, on_close)


async def client_send_rpc(dcpair: DataChannelPair, request: ClientEvent, timeout=None):
    if not request:
        return None

    signal_sid = dcpair.signal_sid
    if not dcpair.is_ready():
        log(signal_sid, f"dc channel is not ready")
        raise Exception("dc channel closed")

    sender, receiver = dcpair.get_pair()
    rpc_future = asyncio.Future()

    def on_message(msg):
        if not isinstance(msg, str):
            return
        if rpc_future.cancelled():
            return
        response = ClientEvent.from_json(msg)
        if response.tid == request.tid:
            rpc_future.set_result(response)

    receiver.add_listener('message', on_message)
    try:
        log(signal_sid, f"dc {sender.label} >>> {request.to_json()}")
        sender.send(request.to_json())
        await asyncio.wait_for(rpc_future, timeout or 30)
    except asyncio.TimeoutError:
        pass
    finally:
        receiver.remove_listener('message', on_message)

    return None if rpc_future.cancelled() else rpc_future.result()


async def ping_test(channel_pair: DataChannelPair):
    ping = ClientEvent('ping')
    res = None
    while not res or res.type != 'ok':
        await asyncio.sleep(1)
        res = await client_send_rpc(channel_pair, ping, 1)
    return res


async def client_request(event: ClientEvent):
    event.future = asyncio.Future()
    client_queue.put_nowait(event)
    await event.future
    return event.future.result()


async def http_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        request_line = (await reader.readline()).decode()
        method, netloc, _ = request_line.split()
    except Exception:
        writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
        safe_close(writer)
        return

    proxy_logger.info(f"received {method} {netloc}")
    is_connect = method == 'CONNECT'
    client_peername = writer.get_extra_info('peername')
    # https proxy only
    if not is_connect:
        url = urllib.parse.urlparse(netloc)
        netloc = f'{url.netloc}:{url.port or 80}'

    # read all headers
    all_headers = await reader.readuntil(b'\r\n\r\n')

    def reject():
        proxy_logger.info(f'rejected {netloc}')
        writer.write(b"HTTP/1.1 500 Not ready\r\n\r\n")
        safe_close(writer)

    async def handle_dc_open(dcpair: DataChannelPair, tid: str):
        if is_connect:
            http200 = b"HTTP/1.1 200 Connection established\r\n\r\n"
            asyncio.ensure_future(safe_write(writer, http200))
        else:
            dcpair.transfer(tid, request_line.encode())
            dcpair.transfer(tid, all_headers)
        await relay_reader_to_dc(reader, dcpair, tid)

    if bootstrap_pc and bootstrap_pc.connectionState == 'connected':
        # localhost test,
        tid = client_peername[1]
        # get dc pair
        dcpair: DataChannelPair = await client_request(ClientEvent('relay', netloc, tid=tid))
        if not dcpair or not dcpair.is_ready():
            reject()
            return

        @dcpair.receiver.on("message")
        def on_message(msg):
            if isinstance(msg, str):
                return
            pickled = pickle.loads(msg)
            if tid == pickled.tid:
                asyncio.ensure_future(safe_write(writer, pickled.data))

        await handle_dc_open(dcpair, tid)
    else:
        reject()


async def run_proxy(port: int | None):
    await start_client()
    server = await asyncio.start_server(http_handler, port=port)
    addr = server.sockets[0].getsockname()
    proxy_logger.info(f'Local server running on {addr[0]}:{addr[1]}')
    async with server:
        await server.serve_forever()


async def main():
    global bootstrap_pc
    global signal_pc
    try:
        if '--client' in sys.argv:
            await start_client()
        elif '--proxy' in sys.argv:
            proxy_port = os.getenv('PROXY_PORT', 2234)
            await run_proxy(proxy_port)
        else:
            await run_server()
        while True:
            await asyncio.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        if bootstrap_pc:
            await bootstrap_pc.close()
            bootstrap_pc = None
        if signal_pc:
            await signal_pc.close()
            signal_pc = None


if __name__ == '__main__':
    import sys
    from aioice import ice
    ice.CONSENT_INTERVAL = 2
    ice.CONSENT_FAILURES = 5
    client_delay = ice.CONSENT_FAILURES * ice.CONSENT_INTERVAL
    # Setup logging configuration
    debug = '--debug' in sys.argv
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    asyncio.run(main())
