from dataclasses import dataclass

from fastapi import FastAPI, status, Response

app = FastAPI(docs_url=None, redoc_url=None)


@dataclass
class SignalRoom:
    sid: str | None = None
    offer: str | None = None
    answer: str | None = None


room_cache = {}


@app.get('/')
def get_api():
    return Response(status_code=status.HTTP_404_NOT_FOUND)


@app.get('/api/signals/{room_id}')
def get_signal(room_id: str):
    res = room_cache.get(room_id)
    return res if res else Response(status_code=status.HTTP_404_NOT_FOUND)


@app.post('/api/signals/{room_id}')
def set_signal(room_id: str, room: SignalRoom):
    if room_id is None or room is None:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)

    room_cache[room_id] = room
    return Response(status_code=status.HTTP_201_CREATED)
