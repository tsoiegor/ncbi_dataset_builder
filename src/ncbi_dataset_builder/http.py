from __future__ import annotations

import email.utils
import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, ClassVar

from .errors import DownloadError


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.body)


class RequestThrottle:
    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.interval = 1.0 / requests_per_second
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self.interval
        if delay:
            time.sleep(delay)


class HttpClient:
    """Small dependency-free HTTP client with throttling and bounded retries."""

    RETRYABLE_STATUS: ClassVar[set[int]] = {408, 425, 429, 500, 502, 503, 504}

    def __init__(
        self,
        *,
        user_agent: str,
        requests_per_second: float = 3.0,
        retries: int = 5,
        timeout_seconds: float = 60.0,
    ) -> None:
        self.user_agent = user_agent
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.throttle = RequestThrottle(requests_per_second)

    @staticmethod
    def _retry_after(headers: Any) -> float | None:
        value = headers.get("Retry-After") if headers else None
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(value)
                return max(0.0, parsed.timestamp() - time.time())
            except (TypeError, ValueError):
                return None

    def request(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        if params:
            separator = "&" if "?" in url else "?"
            url = url + separator + urllib.parse.urlencode(params, doseq=True)
        request_headers = {"User-Agent": self.user_agent, "Accept-Encoding": "identity"}
        request_headers.update(headers or {})
        last_error: BaseException | None = None
        for attempt in range(self.retries + 1):
            self.throttle.wait()
            request = urllib.request.Request(url, headers=request_headers)
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    return HttpResponse(
                        url=response.geturl(),
                        status=response.status,
                        headers=dict(response.headers.items()),
                        body=response.read(),
                    )
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code not in self.RETRYABLE_STATUS or attempt == self.retries:
                    body = exc.read().decode("utf-8", errors="replace")[-2000:]
                    raise DownloadError(f"HTTP {exc.code} for {url}: {body}") from exc
                retry_after = self._retry_after(exc.headers)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last_error = exc
                if attempt == self.retries:
                    break
                retry_after = None
            delay = retry_after if retry_after is not None else min(30.0, 0.75 * (2**attempt))
            time.sleep(delay + random.uniform(0.0, min(1.0, delay / 4)))
        raise DownloadError(
            f"Request failed after {self.retries + 1} attempts: {url}"
        ) from last_error
