import asyncio
import httpx
import logging

from aiortc import RTCDataChannel, RTCPeerConnection, RTCSessionDescription, RTCConfiguration

#iceServers=[RTCIceServer('stun:stun.cloudflare.com:3478')]
default_config = RTCConfiguration()
session_url = 'https://cfstream.lichon.cc/api/sessions'
signal_url = 'https://cfstream.lichon.cc/api/signals/dc1234'

signal_pc = None
relay_pc = None
relay_buffer_size = 4096

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


async def handle_relay_request(sid: str, dc: RTCDataChannel):
    relay_log(sid, f"starting relay to {dc.label}")
    host, port = dc.label.split(':')
    writer = None
    buffer = []

    @dc.on('message')
    def on_message(msg):
        if writer:
            writer.write(msg)
        else:
            buffer.append(msg)

    @dc.on('close')
    def on_close():
        if writer:
            writer.close()

    try:
        reader, writer = await asyncio.open_connection(host, port)
        relay_log(sid, f'connected to {host}:{port}')

        for b in buffer:
            writer.write(b)
        await writer.drain()
        buffer.clear()

        while True:
            data = await reader.read(relay_buffer_size)
            if not data:
                break
            dc.send(data)
    except Exception as e:
        relay_log(sid, f'Error in TCP client: {e}')
        raise
    finally:
        relay_log(sid, f'closing to {host}:{port}')
        writer.close()
        dc.close()

async def start_relay_server(sid: str, offer: str):
    global relay_pc
    relay_log(sid, "starting relay dc")
    relay_pc = pc = RTCPeerConnection(default_config)
    bootstrap_dc = pc.createDataChannel("bootstrap")

    @pc.on("connectionstatechange")
    def on_connection_state():
        relay_log(sid, f"{pc.connectionState}")

    @pc.on("datachannel")
    def on_new_channel(dc: RTCDataChannel):
        relay_log(sid, f"new dc request {dc.label}")
        if (dc.label == "client bootstrap"):
            return
        asyncio.create_task(handle_relay_request(sid, dc))

    @bootstrap_dc.on("open")
    def on_open():
        relay_log(sid, "channel open, restarting signal")
        asyncio.get_event_loop().create_task(run_signal())

    await pc.setLocalDescription(await pc.createOffer())
    proxy_sdp = pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:passive')
    async with httpx.AsyncClient() as client:
        await client.post(signal_url, json={'sid': sid, 'answer': proxy_sdp})

    await pc.setRemoteDescription(RTCSessionDescription(offer, 'answer'))


async def check_client_offer(sid: str):
    global relay_pc
    relay_log(sid, "checking client offer")
    async with httpx.AsyncClient() as client:
        resp = await client.get(signal_url)
        signal = resp.json()
        offer = signal.get('offer')
        current_sid = signal.get('sid')
        if offer and current_sid == sid:
            relay_log(sid, "offer ready")
            await start_relay_server(sid, offer)
        else:
            relay_log(sid, f"retry in 2s")
            later = lambda: asyncio.ensure_future(check_client_offer(sid))
            asyncio.get_event_loop().call_later(2, later)

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
        if current_sid != sid or not answer:
            client_log(sid, f"answer is not ready current sid {current_sid}")
            later = lambda: asyncio.ensure_future(client_connect(sid))
            asyncio.get_event_loop().call_later(2, later)
            return

        await signal_pc.setRemoteDescription(RTCSessionDescription(answer, 'answer'))


async def start_client():
    global signal_pc
    signal_pc = RTCPeerConnection(default_config)
    sid = None

    @signal_pc.on("connectionstatechange")
    def on_connection_state():
        client_log(sid, f"{signal_pc.connectionState}")

    async with httpx.AsyncClient() as client:
        resp = await client.get(signal_url)
        if resp.status_code != 200:
            return

        signal = resp.json()
        sid = signal.get('sid')
        if not sid:
            # retry later
            return

        signal_pc.createDataChannel("client bootstrap")
        await signal_pc.setLocalDescription(await signal_pc.createOffer())
        localSdp = signal_pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:active')

        post = client.post(signal_url, json={'sid': sid, 'offer': localSdp}, timeout=30)
        patch = client.patch(f'{session_url}/{sid}', content=localSdp, timeout=30)
        await asyncio.gather(post, patch)
        client_log(sid, f'kick signal')

    await asyncio.sleep(10)
    await client_connect(sid)


async def handle_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global signal_pc
    request = await reader.readuntil(b'\r\n\r\n')
    headers = request.decode()
    
    request_line = headers.split('\n')[0]
    method, path, _ = request_line.split()
    
    proxy_logger.info(f"Received {method} request for {path}")

    async def handle_body(dc: RTCDataChannel):
        while not reader.at_eof():
            body = await reader.read(relay_buffer_size)
            dc.send(body)
            await asyncio.sleep(0.01)
        proxy_logger.info(f"body finished...")
    
    # request new dc
    if signal_pc and signal_pc.connectionState == 'connected':
        dc = signal_pc.createDataChannel(f'{path}')

        @dc.on("open")
        def on_open():
            proxy_logger.info(f"open dc for {path}")
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            asyncio.ensure_future(writer.drain())
            asyncio.create_task(handle_body(dc))

        @dc.on("close")
        def on_close():
            proxy_logger.info(f"close dc for {path}")
            writer.close()

        @dc.on("message")
        def on_message(data):
            writer.write(data)
    else:
        writer.write(b"HTTP/1.1 500 OK\r\n\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()


async def run_server():
    await start_client()
    server = await asyncio.start_server(handle_request, port=1234)
    addr = server.sockets[0].getsockname()
    proxy_logger.info(f'Local server running on {addr[0]}:{addr[1]}')

    async with server:
        await server.serve_forever() 


async def main():
    global signal_pc
    try:
        if '--client' in sys.argv:
            await start_client()
        elif '--http' in sys.argv:
            await run_server()
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
    ice.CONSENT_INTERVAL = 3
    ice.CONSENT_FAILURES = 3
    # Setup logging configuration
    debug = '--debug' in sys.argv
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    asyncio.run(main())
