from __future__ import annotations

import email.utils
import json
import logging
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, ClassVar

from ..errors import DownloadError

LOGGER = logging.getLogger("ncbi_dataset_builder.http")


@dataclass(frozen=True)
class HttpResponse:
    """Store one HTTP response.

    Args:
        url: Final response URL after redirects.
        status: HTTP status code.
        headers: Response headers.
        body: Unmodified response bytes.
    """

    url: str
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def text(self) -> str:
        """Decode the response body as UTF-8 with replacement for invalid bytes."""

        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        """Decode and return the response body as JSON."""

        return json.loads(self.body)


class RequestThrottle:
    """Serialize callers to a configured maximum request rate."""

    def __init__(self, requests_per_second: float) -> None:
        """Set the positive maximum *requests_per_second*."""

        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.interval = 1.0 / requests_per_second
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        """Block until the next request is permitted by the shared rate limit."""

        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self.interval
        if delay:
            time.sleep(delay)


class HttpClient:
    """Make throttled HTTP requests with bounded retries and timeouts."""

    RETRYABLE_STATUS: ClassVar[set[int]] = {408, 425, 429, 500, 502, 503, 504}

    def __init__(
        self,
        *,
        user_agent: str,
        requests_per_second: float = 3.0,
        retries: int = 5,
        timeout_seconds: float = 60.0,
    ) -> None:
        """Configure identity, rate, retry count, and per-request timeout.

        *user_agent* identifies the caller; *requests_per_second* limits the
        shared rate; *retries* counts retries after the first attempt; and
        *timeout_seconds* bounds each attempt.
        """

        self.user_agent = user_agent
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.throttle = RequestThrottle(requests_per_second)

    @staticmethod
    def _retry_after(headers: Any) -> float | None:
        """Parse a numeric or HTTP-date ``Retry-After`` value from *headers*."""

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
        """GET *url* with optional query *params* and additional request *headers*."""

        if params:
            separator = "&" if "?" in url else "?"
            url = url + separator + urllib.parse.urlencode(params, doseq=True)
        request_headers = {"User-Agent": self.user_agent, "Accept-Encoding": "identity"}
        request_headers.update(headers or {})
        last_error: BaseException | None = None
        for attempt in range(self.retries + 1):
            self.throttle.wait()
            request = urllib.request.Request(url, headers=request_headers)
            LOGGER.debug("HTTP request attempt %d/%d: %s", attempt + 1, self.retries + 1, url)
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
            LOGGER.warning(
                "Retry HTTP request in %.1fs after attempt %d/%d: %s",
                delay,
                attempt + 1,
                self.retries + 1,
                url,
            )
            time.sleep(delay + random.uniform(0.0, min(1.0, delay / 4)))
        raise DownloadError(
            f"Request failed after {self.retries + 1} attempts: {url}"
        ) from last_error
