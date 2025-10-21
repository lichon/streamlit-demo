from dataclasses import dataclass, asdict
from typing import Optional
import uuid
import asyncio
import logging

logger = logging.getLogger('tunnel')


def log(trace_tag: str, msg: str):
    logger.info(msg=f'{trace_tag} {msg}')


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

    def reject(self, msg: str = 'Not ready'):
        if self.writer:
            self.writer.write(f'HTTP/1.1 500 {msg}\r\n\r\n'.encode())
            safe_close(self.writer)
        log(f'{self.tid} {self.uri}', f'rejected: {msg}')


class ProxyPeer:
    ''' proxy peer base class '''
    peer_id: str = str(uuid.uuid1())

    async def start(self):
        pass

    async def stop(self):
        pass

    def connected(self):
        return True

    async def recover(self):
        pass

    async def do_request(self, req: LocalRequest, timeout: int = 60):
        pass
