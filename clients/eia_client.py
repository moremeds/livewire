"""EIA API v2 client: every row of one route's data endpoint, paginated.

Three measured facts shape it (docs/audits/eia/EIA_API_EXPLORATION.md):

- A JSON response carries at most 5,000 rows; the rest is reached by `offset`.
  Offset paging is only exact under a total order, so callers pass a sort that
  covers the whole key, and the row count is checked against EIA's own `total`.
- Uncompressed, a 5,000-row page took 10-36 s from the mini; gzip made it ~1 s
  (1.17 MB -> 46 KB). The header is set explicitly so no client change loses it.
- EIA throttles (burst < 5/s, sustained < ~9,000/h) by suspending the key for
  seconds to minutes, then lifting it; 429 is therefore retried here.
"""

from __future__ import annotations

import gzip
import os
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from clients.constants import declared
from clients.http_retry import get_with_retry

EIA_BASE_URL = "https://api.eia.gov/v2"
PAGE_ROWS = 5000  # EIA's maximum rows per JSON response


@dataclass(frozen=True)
class EiaPage:
    """One response, kept as evidence: EIA serves no vintages, so this is the only as-of record."""

    url: str  # the request URL without the api_key
    body_gzip: bytes  # the decoded body, gzip-compressed with mtime=0 (deterministic)
    retrieved_at: datetime


class EiaClient:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        http_client: Any | None = None,
        base_url: str = EIA_BASE_URL,
        sleep: Callable[[float], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        key = api_key or os.environ.get("EIA_API_KEY")
        if not key:
            raise ValueError("EIA_API_KEY environment variable is not set")
        self.api_key: str = key
        self._http = http_client or httpx
        self._base_url = base_url.rstrip("/")
        self._sleep = sleep
        self._clock = clock
        self._last_request: float | None = None

    def _pace(self) -> None:
        interval = float(declared("eia_min_request_interval_s"))
        if self._last_request is not None:
            wait = interval - (self._clock() - self._last_request)
            if wait > 0:
                (self._sleep or time.sleep)(wait)
        self._last_request = self._clock()

    def fetch(
        self,
        route: str,
        *,
        frequency: str,
        start: str,
        end: str,
        sort_columns: Sequence[str],
        data_columns: Sequence[str] = ("value",),
    ) -> tuple[list[dict], list[EiaPage]]:
        """Every row of `route` in [start, end], and the pages they came from.

        Raises ValueError when the rows received do not add up to EIA's `total`,
        or `total` moves between pages (EIA published mid-read, so offsets
        shifted): a short read must fail, never publish as complete.
        """
        url = f"{self._base_url}/{route.strip('/')}/data/"
        query: list[tuple[str, str | int]] = [("frequency", frequency), ("start", start), ("end", end)]
        query += [("data[]", column) for column in data_columns]
        for index, column in enumerate(sort_columns):
            query += [(f"sort[{index}][column]", column), (f"sort[{index}][direction]", "asc")]

        rows: list[dict] = []
        pages: list[EiaPage] = []
        total: int | None = None
        while total is None or len(rows) < total:
            params = [*query, ("offset", len(rows)), ("length", PAGE_ROWS)]
            self._pace()
            response = get_with_retry(
                lambda params=params: self._http.get(
                    url,
                    params=[*params, ("api_key", self.api_key)],
                    headers={"Accept-Encoding": "gzip"},
                    timeout=60,
                ),
                attempts=int(declared("eia_retry_attempts")),
                backoff_s=float(declared("eia_retry_backoff_s")),
                sleep=self._sleep,
                retry_statuses=frozenset({429}),
            )
            # The body is kept as evidence. EIA's `request` echo omits api_key
            # (checked 2026-09-23); if that ever changes, fail rather than write
            # the key into the lake.
            if self.api_key.encode() in response.content:
                raise ValueError(f"EIA {route}: response body contains the api key; not keeping it")
            body = response.json().get("response")
            if not isinstance(body, dict) or "total" not in body:
                raise ValueError(f"EIA {route}: no response block in {response.text[:200]!r}")
            page_total = int(body["total"])
            if total is not None and page_total != total:
                raise ValueError(f"EIA {route}: total moved from {total} to {page_total} mid-read")
            total = page_total
            pages.append(
                EiaPage(
                    url=str(httpx.URL(url, params=params)),
                    body_gzip=gzip.compress(response.content, mtime=0),
                    retrieved_at=datetime.now(UTC),
                )
            )
            data = body.get("data") or []
            if not data and len(rows) < total:
                raise ValueError(f"EIA {route}: empty page at offset {len(rows)} of {total}")
            rows.extend(data)
        if len(rows) != total:
            raise ValueError(f"EIA {route}: received {len(rows)} rows, total says {total}")
        return rows, pages
