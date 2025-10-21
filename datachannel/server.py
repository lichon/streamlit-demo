import logging
import asyncio
import sys

from http_peer import HttpPeer
from proxy_peer import LocalRequest, ProxyPeer, safe_close


class HttpServer:
    ''' http proxy '''

    def __init__(self, endpoint: ProxyPeer = None):
        self.logger = logging.getLogger('http')
        self.endpoint = endpoint
        self.http_peer = HttpPeer()
        self.switch_peer = False

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peername = writer.get_extra_info('peername')
        if peername is not None and len(peername) >= 2:
            client_addr, client_port = peername[:2]
        else:
            client_addr, client_port = 'unknown', 0
        tag = f'{client_addr}:{client_port}'
        self.logger.info(f'{tag} new connection')
        try:
            request_line = (await reader.readline()).decode()
            method, uri, _ = request_line.split()
        except Exception:
            writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
            safe_close(writer)
            return

        # switch between realtime and http peer
        self.switch_peer = not self.switch_peer
        peer = self.endpoint if self.endpoint and self.switch_peer else self.http_peer
        self.logger.info(f'{tag} received {method} {uri} by {peer.__class__.__name__}')
        # endpoint readiness check
        if method == 'GET' and uri == '/':
            if self.endpoint and self.endpoint.connected():
                writer.write(b'HTTP/1.1 200 OK\r\n\r\n')
            else:
                if self.endpoint:
                    await self.endpoint.recover()
                writer.write(b'HTTP/1.1 500 Not ready\r\n\r\n')
            safe_close(writer)
            return
        # custom testing
        if method == 'GET' and uri == '/ping':
            res = await self.endpoint.do_request(LocalRequest('ping'))
            writer.write(f'HTTP/1.1 200 {res}\r\n\r\n'.encode())
            safe_close(writer)
            return
        # proxy over websocket
        if method == 'GET' and uri.startswith('/connect/'):
            await self.http_peer.do_request(
                LocalRequest(method, uri, tag, reader, writer)
            )
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
                await peer.do_request(
                    LocalRequest(method, uri, tag, reader, writer),
                    timeout=120
                )
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

    async def start(self, port: int = 2234):
        server = await asyncio.start_server(self.handle_request, port=port)
        addr = server.sockets[0].getsockname()
        self.logger.info(f'Local server running on {addr[0]}:{addr[1]}')

        if self.endpoint:
            await self.endpoint.start()
        async with server:
            await server.serve_forever()

    async def stop(self):
        if self.endpoint:
            await self.endpoint.stop()


async def main(peer: ProxyPeer = None):
    process = None
    try:
        process = HttpServer(peer)
        await process.start(port=2234)
    except KeyboardInterrupt:
        pass
    finally:
        await process.stop()

if __name__ == '__main__':
    # Setup logging configuration
    debug = '--debug' in sys.argv
    use_rtc = '--rtc' in sys.argv
    rtc_peer = None
    if use_rtc:
        from rtc_peer import RtcPeer
        from aioice import ice
        ice.CONSENT_INTERVAL = 2
        ice.CONSENT_FAILURES = 5
        rtc_peer = RtcPeer()
        if not debug:
            logging.getLogger('realtime._async.client').setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    asyncio.run(main(rtc_peer))
