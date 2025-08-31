import urllib.parse
import asyncio
import httpx
import logging
import os

from aiortc import RTCDataChannel, RTCPeerConnection, RTCSessionDescription, RTCConfiguration

signal_room = os.environ.get('SIGNAL_ROOM', 'defaultsignal')

#iceServers=[RTCIceServer('stun:stun.cloudflare.com:3478')]
default_config = RTCConfiguration()
session_url = f'https://cfstream.lichon.cc/api/sessions'
signal_url = f'https://cfstream.lichon.cc/api/signals/{signal_room}'

signal_pc = None
relay_buffer_size = 4096
client_delay = 30

proxy_logger = logging.getLogger('proxy ')
relay_logger = logging.getLogger('relay ')
signal_logger = logging.getLogger('signal')
client_logger = logging.getLogger('client')


def signal_log(sid: str, msg: str):
    signal_logger.info(msg=f"{sid} {msg}")

def relay_log(sid: str, msg: str):
    relay_logger.info(msg=f"{sid} {msg}")

def client_log(sid: str, msg: str):
    client_logger.info(msg=f"{sid} {msg}")

async def safe_drain(writer):
    try:
        await writer.drain()
    except Exception:
        writer.close()

async def safe_write_dc_data(writer, data, dc):
    try:
        writer.write(data)
        await writer.drain()
    except Exception:
        writer.close()
        dc.close()

async def handle_relay_dc(sid: str, dc: RTCDataChannel):
    host, port = dc.label.split(':')
    if not host or not port:
        dc.close()
        return

    writer = None
    buffer = []

    @dc.on('message')
    def on_message(msg):
        if writer:
            asyncio.ensure_future(safe_write_dc_data(writer, msg, dc))
        else:
            buffer.append(msg)

    @dc.on('close')
    def on_close():
        relay_log(sid, f'dc {dc.id} closed {dc.label}')
        if writer:
            writer.close()

    try:
        reader, writer = await asyncio.open_connection(host, port)
        relay_log(sid, f'dc {dc.id} connected to {dc.label}')

        for b in buffer: writer.write(b)
        await writer.drain()
        buffer.clear()

        while True:
            if (dc.bufferedAmount > relay_buffer_size * 100):
                relay_log(sid, f'client receive buffer full, waiting')
                await asyncio.sleep(0.1)
            else:
                await asyncio.sleep(0)
            data = await reader.read(relay_buffer_size)
            if not data:
                break
            dc.send(data)

    except Exception as e:
        relay_log(sid, f'dc {dc.id} error: {e}')
        raise
    finally:
        relay_log(sid, f'dc {dc.id} final clear')
        writer.close()
        dc.close()

async def start_relay_dc(sid: str, offer: str):
    relay_log(sid, "starting relay dc")
    pc = RTCPeerConnection(default_config)
    pc.createDataChannel("bootstrap")

    @pc.on("connectionstatechange")
    def on_connection_state():
        relay_log(sid, f"{pc.connectionState}")
        if pc.connectionState in ["failed", "closed", "connected"]:
            asyncio.get_event_loop().create_task(run_signal())

    @pc.on("datachannel")
    def on_new_channel(dc: RTCDataChannel):
        relay_log(sid, f"dc {dc.id} request {dc.label}")
        if (dc.label == "client bootstrap"):
            return
        asyncio.create_task(handle_relay_dc(sid, dc))

    await pc.setLocalDescription(await pc.createOffer())
    relay_sdp = pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:passive')
    async with httpx.AsyncClient() as client:
        await client.post(signal_url, json={'sid': sid, 'answer': relay_sdp})

    await pc.setRemoteDescription(RTCSessionDescription(offer, 'answer'))


async def check_client_offer(sid: str):
    relay_log(sid, "checking client offer")
    async with httpx.AsyncClient() as client:
        resp = await client.get(signal_url)
        signal = resp.json()
        offer = signal.get('offer')
        current_sid = signal.get('sid')
        if offer and current_sid == sid:
            relay_log(sid, "offer ready")
            await start_relay_dc(sid, offer)
        else:
            relay_log(sid, f"retry in 10s")
            later = lambda: asyncio.ensure_future(check_client_offer(sid))
            asyncio.get_event_loop().call_later(10, later)

async def run_signal():
    global signal_pc
    signal_pc = RTCPeerConnection(default_config)
    signal_pc.createDataChannel("signal")
    sid = None

    @signal_pc.on("connectionstatechange")
    def on_connection_state():
        signal_log(sid, f"{signal_pc.connectionState}")
        if signal_pc.connectionState in ['failed', 'closed']:
            prepare = lambda: asyncio.ensure_future(check_client_offer(sid))
            asyncio.get_event_loop().call_soon(prepare)

    await signal_pc.setLocalDescription(await signal_pc.createOffer())

    async with httpx.AsyncClient() as client:
        resp = await client.post(session_url, content=signal_pc.localDescription.sdp)
        sid = resp.headers['Location'].split('/')[-1]
        sdp = resp.text
        await signal_pc.setRemoteDescription(RTCSessionDescription(sdp, 'answer'))
        await client.post(signal_url, json={'sid': sid})
        signal_log(sid, f"session ready")


async def client_connect(sid : str):
    global signal_pc
    if not signal_pc or not sid:
        client_log(sid, "pc or sid is None")
        return

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(signal_url)
        if resp.status_code != 200:
            return

        signal = resp.json()
        current_sid = signal.get('sid')
        answer = signal.get('answer')
        if current_sid != sid:
            client_log(sid, f"sid updated to {current_sid}")
            later = lambda: asyncio.ensure_future(start_client())
            asyncio.get_event_loop().call_later(1, later)
            return
        if not answer:
            client_log(sid, f"answer is not ready")
            later = lambda: asyncio.ensure_future(client_connect(sid))
            asyncio.get_event_loop().call_later(10, later)
            return

        await signal_pc.setRemoteDescription(RTCSessionDescription(answer, 'answer'))


async def start_client():
    global signal_pc
    if signal_pc:
        await signal_pc.close()
    start_latter = lambda: asyncio.ensure_future(start_client())

    signal_pc = RTCPeerConnection(default_config)
    sid = None

    @signal_pc.on("connectionstatechange")
    def on_connection_state():
        client_log(sid, f"{signal_pc.connectionState}")

    async with httpx.AsyncClient() as client:
        resp = await client.get(signal_url)
        if resp.status_code != 200:
            client_log(sid, 'signal room may be invalid')
            asyncio.get_event_loop().call_later(10, start_latter)
            return

        signal = resp.json()
        sid = signal.get('sid')
        if not sid or signal.get('offer') or signal.get('answer'):
            client_log(sid, 'signal is connecting by other client')
            asyncio.get_event_loop().call_later(2, start_latter)
            return

        signal_pc.createDataChannel("client bootstrap")
        await signal_pc.setLocalDescription(await signal_pc.createOffer())
        localSdp = signal_pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:active')

        post = client.post(signal_url, json={'sid': sid, 'offer': localSdp}, timeout=30)
        patch = client.patch(f'{session_url}/{sid}', content=localSdp, timeout=30)
        await asyncio.gather(post, patch)
        client_log(sid, f'kick signal')

    await asyncio.sleep(client_delay)
    await client_connect(sid)


async def handle_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global signal_pc
    try:
        request_line = (await reader.readline()).decode()
        method, netloc, _ = request_line.split()
    except Exception as e:
        writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
        writer.close()
        return
    
    proxy_logger.info(f"Received {method} {netloc}")
    is_connect = method == 'CONNECT'
    # https proxy only
    if not is_connect:
        url = urllib.parse.urlparse(netloc)
        netloc = f'{url.netloc}:{url.port or 80}'

    # read all headers
    all_headers = await reader.readuntil(b'\r\n\r\n')

    async def handle_body(dc: RTCDataChannel):
        if not is_connect:
            dc.send(request_line.encode())
            dc.send(all_headers)
        while not reader.at_eof():
            if (dc.bufferedAmount > relay_buffer_size * 100):
                proxy_logger.info(f'client receive buffer full, waiting')
                await asyncio.sleep(0.1)
            else:
                await asyncio.sleep(0)
            body = await reader.read(relay_buffer_size)
            if dc.readyState != "open":
                writer.close()
                break
            dc.send(body)
            await asyncio.sleep(0)
    
    # request new dc
    if signal_pc and signal_pc.connectionState == 'connected':
        dc = signal_pc.createDataChannel(f'{netloc}')

        @dc.on("open")
        def on_open():
            proxy_logger.info(f"dc {dc.id} open for {netloc}")
            if is_connect:
                writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                asyncio.ensure_future(safe_drain(writer))
            asyncio.create_task(handle_body(dc))

        @dc.on("close")
        def on_close():
            proxy_logger.info(f"dc {dc.id} close for {netloc}")
            writer.close()

        @dc.on("message")
        def on_message(msg):
            #proxy_logger.info(f"dc {dc.id} recv <<< len {len(msg)} {dc.label}")
            asyncio.ensure_future(safe_write_dc_data(writer, msg, dc))
    else:
        proxy_logger.info(f'rejecting {netloc}')
        writer.write(b"HTTP/1.1 500 Not ready\r\n\r\n")
        await writer.drain()
        writer.close()


async def run_proxy(port: int | None):
    await start_client()
    server = await asyncio.start_server(handle_request, port=port)
    addr = server.sockets[0].getsockname()
    proxy_logger.info(f'Local server running on {addr[0]}:{addr[1]}')

    async with server:
        await server.serve_forever() 


async def main():
    global signal_pc
    try:
        if '--client' in sys.argv:
            await start_client()
        elif '--proxy' in sys.argv:
            proxy_port = os.getenv('PROXY_PORT', 1234)
            await run_proxy(proxy_port)
        else:
            await run_signal()
        while True:
            await asyncio.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        if signal_pc:
            await signal_pc.close()
            signal_pc = None


if __name__ == '__main__':
    import sys
    from aioice import ice
    ice.CONSENT_INTERVAL = 5
    ice.CONSENT_FAILURES = 6
    client_delay = ice.CONSENT_FAILURES * ice.CONSENT_INTERVAL
    # Setup logging configuration
    debug = '--debug' in sys.argv
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    asyncio.run(main())
