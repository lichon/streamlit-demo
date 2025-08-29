import asyncio
import httpx
import logging

from aiortc import RTCIceServer, RTCPeerConnection, RTCSessionDescription, RTCConfiguration

#iceServers=[RTCIceServer('stun:stun.cloudflare.com:3478')]
default_config = RTCConfiguration()
session_url = 'https://cfstream.lichon.cc/api/sessions'
signal_url = 'https://cfstream.lichon.cc/api/signals/dc1234'

signal_pc = None
proxy_pc = None

signal_logger = logging.getLogger('signal')
proxy_logger = logging.getLogger('proxy')
client_logger = logging.getLogger('client')


def signal_log(sid: str, msg: str):
    signal_logger.info(msg=f"{sid} {msg}")

def proxy_log(sid: str, msg: str):
    proxy_logger.info(msg=f"{sid} {msg}")

def client_log(sid: str, msg: str):
    client_logger.info(msg=f"{sid} {msg}")


async def start_proxy(sid: str, offer: str):
    global proxy_pc
    proxy_log(sid, "starting proxy")
    proxy_pc = pc = RTCPeerConnection(default_config)
    channel = pc.createDataChannel("proxy")

    @pc.on("connectionstatechange")
    def on_connection_state():
        proxy_log(sid, f"{pc.connectionState}")

    @pc.on("iceconnectionstatechange")
    def on_ice_connection_state():
        signal_log(sid, f"ice {pc.iceConnectionState}")

    @channel.on("open")
    def on_open():
        proxy_log(sid, f"channel opened")

    @channel.on("message")
    def on_message(message):
        proxy_log(sid, f"channel msg < {message}")

    await pc.setLocalDescription(await pc.createOffer())
    proxy_sdp = pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:passive')
    async with httpx.AsyncClient() as client:
        await client.post(signal_url, json={'sid': sid, 'answer': proxy_sdp})

    await pc.setRemoteDescription(RTCSessionDescription(offer, 'answer'))


async def check_client_offer(sid: str):
    global proxy_pc
    proxy_log(sid, "checking client offer")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(signal_url)
            signal = resp.json()
            offer = signal.get('offer')
            current_sid = signal.get('sid')
            if offer and current_sid == sid:
                proxy_log(sid, "offer ready")
                await start_proxy(sid, offer)
            else:
                proxy_log(sid, f"retry in 2s")
                later = lambda: asyncio.ensure_future(check_client_offer(sid))
                asyncio.get_event_loop().call_later(2, later)
    finally:
        signal_log('new', "skip restarting signal")
        #await run_signal()

async def run_signal():
    global signal_pc
    signal_pc = RTCPeerConnection(default_config)
    channel = signal_pc.createDataChannel("signal")
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
    if not signal_pc:
        client_log(sid, "pc is None")
        return

    async with httpx.AsyncClient() as client:
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


async def run_client():
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

        signal_pc.createDataChannel("proxy")
        await signal_pc.setLocalDescription(await signal_pc.createOffer())
        localSdp = signal_pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:active')

        signal_update = client.post(signal_url, json={'sid': sid, 'offer': localSdp})
        kick_session = client.patch(f'{session_url}/{sid}', content=localSdp)
        await signal_update
        await kick_session
        client_log(sid, f'kick signal')

        await asyncio.sleep(10)
        await client_connect(sid)


async def kill_session_test(sid: str):
    global signal_pc
    signal_pc = RTCPeerConnection(default_config)
    signal_pc.createDataChannel("bootstrap")
    offer = await signal_pc.createOffer()
    client_log(sid, f'{offer}')
    async with httpx.AsyncClient() as client:
        # kick signal session
        await client.patch(f'{session_url}/{sid}', content=offer.sdp)


async def stun_test(hostport: str):
    from aiortc import RTCIceGatherer
    iceGatherer = RTCIceGatherer(
        iceServers=[RTCIceServer(f'stun:{hostport}')]
        if hostport else default_config.iceServers
    )
    await iceGatherer.gather()


async def main():
    global signal_pc
    try:
        if '--client' in sys.argv:
            await run_client()
        elif '--stun' in sys.argv:
            await stun_test(sys.argv[2] if len(sys.argv) > 2 else None)
        elif '--test' in sys.argv:
            await kill_session_test(sys.argv[2])
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
