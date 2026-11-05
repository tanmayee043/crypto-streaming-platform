"""Per-IP limits for the public API, so one visitor can't overload the small free VM."""

import json
import time

# not limited: uptime monitors and Prometheus check these often, and Caddy keeps /metrics private
EXEMPT_PATHS = ("/healthz", "/readyz", "/metrics")
MAX_TRACKED_IPS = 10_000


class TokenBucket:
    """Each IP has a bucket of `per_minute` tokens that slowly refills; each request takes one.
    Short bursts (loading the page) are fine; sustained hammering runs out."""

    def __init__(self, per_minute, clock=time.monotonic):
        self.capacity = per_minute
        self.refill_per_second = per_minute / 60
        self.clock = clock
        self.buckets = {}  # ip -> (tokens, last seen)

    def allow(self, ip):
        now = self.clock()
        tokens, last = self.buckets.get(ip, (self.capacity, now))
        tokens = min(self.capacity, tokens + (now - last) * self.refill_per_second)
        allowed = tokens >= 1
        self.buckets[ip] = (tokens - 1 if allowed else tokens, now)
        if len(self.buckets) > MAX_TRACKED_IPS:  # keep memory bounded: forget the longest-idle half
            for old in sorted(self.buckets, key=lambda k: self.buckets[k][1])[: MAX_TRACKED_IPS // 2]:
                del self.buckets[old]
        return allowed


class RateLimitMiddleware:
    def __init__(self, app, per_minute):
        self.app = app
        self.bucket = TokenBucket(per_minute) if per_minute > 0 else None  # 0 turns it off

    async def __call__(self, scope, receive, send):
        if self.bucket is None or scope["type"] != "http" or scope["path"].startswith(EXEMPT_PATHS):
            await self.app(scope, receive, send)
            return
        client = scope.get("client")
        if self.bucket.allow(client[0] if client else "unknown"):
            await self.app(scope, receive, send)
            return
        await too_many_requests(send, "Too many requests. Please slow down and try again shortly.")


async def too_many_requests(send, message):
    body = json.dumps({"detail": message}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 429,
            "headers": [(b"content-type", b"application/json"), (b"retry-after", b"30")],
        }
    )
    await send({"type": "http.response.body", "body": body})
