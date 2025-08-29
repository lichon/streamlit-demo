import asyncio
import httpx
import logging

from aiortc import RTCIceServer, RTCPeerConnection, RTCSessionDescription, RTCConfiguration

ice_server = 'stun:stun.cloudflare.com:3478'
default_config = RTCConfiguration([RTCIceServer(ice_server)])
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


async def prepare_proxy_offer(signal_sid: str):
    global proxy_pc
    proxy_log(signal_sid, "pc preparing")
    proxy_pc = pc = RTCPeerConnection(default_config)
    channel = pc.createDataChannel("proxy")

    @pc.on("connectionstatechange")
    def on_connection_state():
        proxy_log(signal_sid, f"{pc.connectionState}")

    @channel.on("open")
    def on_open():
        proxy_log(signal_sid, f"channel opened")

    @channel.on("message")
    def on_message(message):
        proxy_log(signal_sid, f"channel msg < {message}")

    await pc.setLocalDescription(await pc.createOffer())
    async with httpx.AsyncClient() as client:
        await client.post(signal_url, json={'notifySid': signal_sid, 'offer': pc.localDescription.sdp})
    proxy_log(signal_sid, f"pc ready")
    return pc


async def setup_proxy_offer(sid: str):
    global proxy_pc
    pc = proxy_pc
    proxy_log(sid, "setting up")
    try:
        if not pc:
            proxy_log(sid, "pc is None")
            return
        async with httpx.AsyncClient() as client:
            resp = await client.get(signal_url)
            signal = resp.json()
            answer = signal.get('answer')
            proxy_log(sid, f"remote signal answer {'ready' if answer else 'not ready'}")  
            if answer and sid == signal.get('notifySid'):
                await pc.setRemoteDescription(RTCSessionDescription(answer, 'answer'))
                proxy_log(sid, f"set remote sdp")
    finally:
        signal_log('new', "skip restarting signal")
        #await run_signal()


async def run_client():
    global signal_pc
    signal_pc = RTCPeerConnection(default_config)
    sid = None
    offer = None

    @signal_pc.on("connectionstatechange")
    def on_connection_state():
        client_log(sid, f"{signal_pc.connectionState}")

    async with httpx.AsyncClient() as client:
        resp = await client.get(signal_url)
        if resp.status_code != 200:
            return
        signal = resp.json()
        sid = signal.get('notifySid')
        offer = signal.get('offer')
        if sid and offer:
            signal_pc.createDataChannel("proxy")
            await signal_pc.setLocalDescription(await signal_pc.createOffer())
            localSdp = signal_pc.localDescription.sdp.replace('a=setup:actpass', 'a=setup:passive')
            remoteSdp = offer.replace('a=setup:actpass', 'a=setup:active')
            #answer = await signal_pc.createAnswer()
            #await signal_pc.setLocalDescription(answer)
            await client.post(signal_url, json={'notifySid': sid, 'answer': localSdp})
            # kick signal session
            await client.patch(f'{session_url}/{sid}', content=offer)
            client_log(sid, f'kick signal')

            await asyncio.sleep(30)
            await signal_pc.setRemoteDescription(RTCSessionDescription(remoteSdp, 'answer'))

async def run_test(sid: str):
    global signal_pc
    signal_pc = RTCPeerConnection(default_config)
    signal_pc.createDataChannel("bootstrap")
    offer = await signal_pc.createOffer()
    client_log(sid, f'{offer}')
    async with httpx.AsyncClient() as client:
        # kick signal session
        await client.patch(f'{session_url}/{sid}', content=offer.sdp)


async def run_signal():
    global signal_pc
    signal_pc = RTCPeerConnection(default_config)
    channel = signal_pc.createDataChannel("signal")
    sid = None

    @signal_pc.on("connectionstatechange")
    def on_connection_state():
        signal_log(sid, f"{signal_pc.connectionState}")
        if signal_pc.connectionState in ['failed', 'closed']:
            setup = lambda: asyncio.ensure_future(setup_proxy_offer(sid))
            asyncio.get_event_loop().call_soon(setup)

    @signal_pc.on("iceconnectionstatechange")
    def on_ice_connection_state():
        signal_log(sid, f"ice {signal_pc.iceConnectionState}")

    @channel.on("open")
    def on_open():
        signal_log(sid, f"channel opened")
        asyncio.ensure_future(prepare_proxy_offer(sid))

    await signal_pc.setLocalDescription(await signal_pc.createOffer())

    async with httpx.AsyncClient() as client:
        resp = await client.post(session_url, content=signal_pc.localDescription.sdp)
        sid = resp.headers['Location'].split('/')[-1]
        sdp = resp.text
        await signal_pc.setRemoteDescription(RTCSessionDescription(sdp, 'answer'))
        await client.post(signal_url, json={'notifySid': sid})
        signal_log(sid, f"session ready")


async def main():
    global signal_pc
    try:
        if '--test' in sys.argv:
            await run_test(sys.argv[2])
        elif '--client' in sys.argv:
            await run_client()
        else:
            await run_signal()
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        if signal_pc:
            await signal_pc.close()
            signal_pc = None

if __name__ == '__main__':
    import sys
    # Setup logging configuration
    debug = '--debug' in sys.argv
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    asyncio.run(main())
