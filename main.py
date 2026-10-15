import base64
import math
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Optional

from fastapi import FastAPI, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL_ORDERS = 45          # T
RATE_LIMIT = 16            # R requests ...
WINDOW_SECONDS = 10.0      # ... per 10 seconds
MAX_LIMIT = 100

app = FastAPI(title="Orders API")

# ---------------------------------------------------------------- state
_lock = threading.Lock()
_idempotency: dict[str, dict] = {}     # key -> order
_created_orders: list[dict] = []
_hits: dict[str, deque] = defaultdict(deque)   # client id -> request timestamps


def catalog_order(i: int) -> dict:
    return {"id": i, "status": "confirmed", "item": f"item-{i}", "quantity": 1}


# ---------------------------------------------------------------- rate limiting
@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if request.method == "OPTIONS" or request.url.path == "/health":
        return await call_next(request)

    client_id = request.headers.get("x-client-id") or "anonymous"
    now = time.monotonic()
    with _lock:
        q = _hits[client_id]
        while q and now - q[0] >= WINDOW_SECONDS:
            q.popleft()
        if len(q) >= RATE_LIMIT:
            retry_after = max(1, math.ceil(WINDOW_SECONDS - (now - q[0])))
            return JSONResponse(
                {"detail": "Rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )
        q.append(now)
    return await call_next(request)


# CORS added AFTER the rate limiter so it is the outermost layer
# (429 responses and preflights also carry CORS headers).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotency-Key"],
)


# ---------------------------------------------------------------- routes
@app.get("/")
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    with _lock:
        if idempotency_key and idempotency_key in _idempotency:
            return JSONResponse(_idempotency[idempotency_key], status_code=201)
        order = {
            "id": str(uuid.uuid4()),
            "status": "created",
            **{k: v for k, v in body.items() if k != "id"},
        }
        _created_orders.append(order)
        if idempotency_key:
            _idempotency[idempotency_key] = order
    return JSONResponse(order, status_code=201)


def _encode(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode().rstrip("=")


def _decode(cursor: str) -> int:
    try:
        pad = "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(cursor + pad).decode()
        return max(0, int(raw.split(":", 1)[1]))
    except Exception:
        return 0


@app.get("/orders")
def list_orders(
    limit: int = Query(10),
    cursor: Optional[str] = Query(None),
):
    limit = max(1, min(limit, MAX_LIMIT))
    start = _decode(cursor) if cursor else 0
    end = min(start + limit, TOTAL_ORDERS)
    items = [catalog_order(i) for i in range(start + 1, end + 1)]
    next_cursor = _encode(end) if end < TOTAL_ORDERS else None
    return {"items": items, "next_cursor": next_cursor}
