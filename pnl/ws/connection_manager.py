"""In-memory registry of open WebSocket connections per client_id, used to
push live updates after the initial reconnect snapshot. Provided as-is —
a simple building block, not this stage's exercise (that's
`ReconnectCache` and the Lua script)."""
from __future__ import annotations

from collections import defaultdict

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)

    def connect(self, client_id: str, websocket: WebSocket) -> None:
        self._connections[client_id].add(websocket)

    def disconnect(self, client_id: str, websocket: WebSocket) -> None:
        self._connections[client_id].discard(websocket)
        if not self._connections[client_id]:
            del self._connections[client_id]

    async def send(self, client_id: str, message: dict) -> None:
        for ws in list(self._connections.get(client_id, ())):
            try:
                await ws.send_json(message)
            except Exception:
                self.disconnect(client_id, ws)
