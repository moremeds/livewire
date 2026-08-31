"""Revision-aware canonical storage for split and cash-dividend events."""

from __future__ import annotations

import hashlib
from contextlib import nullcontext
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

from clients.massive_client import MassiveDividend, MassivePageEvidence, MassiveSplit
from clients.parquet_io import publish_parquet, symbol_lock
from clients.symbol_paths import canonical_symbol, encode_symbol

ActionStatus = Literal["active", "corrected", "cancelled"]
ProviderEvent = MassiveSplit | MassiveDividend

# The only provider `reconcile()` speaks for. Rows from any other provider —
# `apply_repairs(provider="yahoo", …)` — are outside what a Massive response
# can say anything about, so a full reconcile must not cancel them.
RECONCILE_PROVIDER = "massive"


@dataclass(frozen=True)
class CorporateAction:
    action_id: str
    provider: str
    provider_event_id: str
    event_revision: int
    supersedes_action_id: str | None
    symbol: str
    action_type: str
    ex_date: date
    split_from: float | None
    split_to: float | None
    cash_amount: float | None
    currency: str | None
    declaration_date: date | None
    record_date: date | None
    pay_date: date | None
    status: ActionStatus
    fetched_at: datetime
    payload_hash: str
    source_ref: str | None = None
    source_hash: str | None = None
    source_fetched_at: datetime | None = None
    source_cursor_identity: str | None = None


@dataclass(frozen=True)
class CorporateActionFetch:
    fetch_id: str
    symbol: str
    fetched_at: datetime
    full_reconcile: bool
    resources: tuple[str, ...]
    source_refs: tuple[str, ...]
    source_hashes: tuple[str, ...]
    cursor_identities: tuple[str, ...]


@dataclass(frozen=True)
class ReconcileResult:
    inserted: int = 0
    revised: int = 0
    cancelled: int = 0
    unchanged: int = 0

    @property
    def changed(self) -> bool:
        return self.inserted + self.revised + self.cancelled > 0


@dataclass(frozen=True)
class SplitAddition:
    """A split to insert from a non-Massive reference (Yahoo). ``split_from``/``split_to``
    follow the store convention: ratio = split_to / split_from (a 2:1 forward split is
    split_from=1, split_to=2; a 10:1 reverse is split_from=10, split_to=1)."""

    ex_date: date
    split_from: float
    split_to: float


@dataclass(frozen=True)
class RepairResult:
    added: int = 0
    cancelled: int = 0

    @property
    def changed(self) -> bool:
        return self.added + self.cancelled > 0


class CorporateActionStore:
    """Publish per-symbol corporate-action histories with retained lineage."""

    schema = pa.schema(
        [
            pa.field("action_id", pa.string(), nullable=False),
            pa.field("provider", pa.string(), nullable=False),
            pa.field("provider_event_id", pa.string(), nullable=False),
            pa.field("event_revision", pa.int32(), nullable=False),
            pa.field("supersedes_action_id", pa.string()),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("action_type", pa.string(), nullable=False),
            pa.field("ex_date", pa.date32(), nullable=False),
            pa.field("split_from", pa.float64()),
            pa.field("split_to", pa.float64()),
            pa.field("cash_amount", pa.float64()),
            pa.field("currency", pa.string()),
            pa.field("declaration_date", pa.date32()),
            pa.field("record_date", pa.date32()),
            pa.field("pay_date", pa.date32()),
            pa.field("status", pa.string(), nullable=False),
            pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("payload_hash", pa.string(), nullable=False),
            pa.field("source_ref", pa.string()),
            pa.field("source_hash", pa.string()),
            pa.field("source_fetched_at", pa.timestamp("us", tz="UTC")),
            pa.field("source_cursor_identity", pa.string()),
        ]
    )
    fetch_schema = pa.schema(
        [
            pa.field("fetch_id", pa.string(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("full_reconcile", pa.bool_(), nullable=False),
            pa.field("resources", pa.list_(pa.string()), nullable=False),
            pa.field("source_refs", pa.list_(pa.string()), nullable=False),
            pa.field("source_hashes", pa.list_(pa.string()), nullable=False),
            pa.field("cursor_identities", pa.list_(pa.string()), nullable=False),
        ]
    )

    def __init__(self, data_lake_root: Path):
        self._root = Path(data_lake_root)

    def path_for(self, symbol: str) -> Path:
        symbol = canonical_symbol(symbol)
        return (
            self._root
            / "bronze"
            / "asset_class=corporate_action"
            / f"symbol={encode_symbol(symbol)}"
            / "events.parquet"
        )

    def fetch_path_for(self, symbol: str) -> Path:
        symbol = canonical_symbol(symbol)
        return (
            self._root
            / "bronze"
            / "asset_class=corporate_action_fetch"
            / f"symbol={encode_symbol(symbol)}"
            / "fetches.parquet"
        )

    def record_fetch(
        self,
        symbol: str,
        pages: list[MassivePageEvidence],
        fetched_at: datetime,
        *,
        full_reconcile: bool,
        dry_run: bool = False,
    ) -> CorporateActionFetch:
        symbol = canonical_symbol(symbol)
        ordered = sorted(pages, key=lambda page: (page.resource, page.cursor_identity, page.ref))
        identity = "|".join(
            [symbol, fetched_at.isoformat(), str(full_reconcile)]
            + [f"{page.resource}:{page.cursor_identity}:{page.sha256}" for page in ordered]
        )
        receipt = CorporateActionFetch(
            fetch_id=hashlib.blake2b(identity.encode(), digest_size=16).hexdigest(),
            symbol=symbol,
            fetched_at=fetched_at,
            full_reconcile=full_reconcile,
            resources=tuple(page.resource for page in ordered),
            source_refs=tuple(page.ref for page in ordered),
            source_hashes=tuple(page.sha256 for page in ordered),
            cursor_identities=tuple(page.cursor_identity for page in ordered),
        )
        if dry_run:
            return receipt
        path = self.fetch_path_for(symbol)
        with symbol_lock(path):
            rows = self._read_fetches(path)
            if all(row.fetch_id != receipt.fetch_id for row in rows):
                rows.append(receipt)
                rows.sort(key=lambda row: row.fetch_id)
                table = pa.Table.from_pylist([asdict(row) for row in rows], schema=self.fetch_schema)
                publish_parquet(path, table, sort_column="fetch_id")
        return receipt

    def fetch_history(self, symbol: str) -> list[CorporateActionFetch]:
        return sorted(self._read_fetches(self.fetch_path_for(symbol)), key=lambda row: (row.fetched_at, row.fetch_id))

    def reconcile(
        self,
        symbol: str,
        events: list[ProviderEvent],
        fetched_at: datetime,
        *,
        full_reconcile: bool = False,
        dry_run: bool = False,
    ) -> ReconcileResult:
        symbol = canonical_symbol(symbol)
        event_ids = [event.provider_event_id for event in events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("duplicate provider event id in reconciliation")
        if any(event.ticker != symbol for event in events):
            raise ValueError("corporate action ticker does not match reconciliation symbol")

        path = self.path_for(symbol)
        lock = nullcontext() if dry_run else symbol_lock(path)
        with lock:
            rows = self._read(path)
            latest = self._latest_by_provider_id(rows)
            inserted = revised = unchanged = cancelled = 0

            for event in events:
                previous = latest.get(event.provider_event_id)
                if previous is None:
                    current = self._from_provider(event, fetched_at, revision=1, supersedes=None)
                    rows.append(current)
                    latest[event.provider_event_id] = current
                    inserted += 1
                elif previous.payload_hash == event.payload_hash and previous.status == "active":
                    unchanged += 1
                else:
                    rows[rows.index(previous)] = replace(previous, status="corrected")
                    current = self._from_provider(
                        event,
                        fetched_at,
                        revision=previous.event_revision + 1,
                        supersedes=previous.action_id,
                    )
                    rows.append(current)
                    latest[event.provider_event_id] = current
                    revised += 1

            if full_reconcile:
                # Scoped to RECONCILE_PROVIDER, because `events` only ever came from
                # that provider. Sweeping every active row meant the Sunday
                # `--full-reconcile` cancelled the yahoo splits `apply_repairs` had
                # just added, every week: 507 of 1,014 were cancelled across
                # 2026-07-19 (418) and 2026-07-26 (89). Absence from a Massive
                # response says nothing about an event Massive was never asked for.
                incoming = set(event_ids)
                for event_id, previous in list(latest.items()):
                    if event_id in incoming or previous.status != "active" or previous.provider != RECONCILE_PROVIDER:
                        continue
                    cancelled_row = replace(
                        previous,
                        action_id=self._action_id(
                            previous.provider,
                            event_id,
                            previous.event_revision + 1,
                            previous.payload_hash,
                        ),
                        event_revision=previous.event_revision + 1,
                        supersedes_action_id=previous.action_id,
                        status="cancelled",
                        fetched_at=fetched_at,
                    )
                    rows.append(cancelled_row)
                    latest[event_id] = cancelled_row
                    cancelled += 1

            result = ReconcileResult(inserted, revised, cancelled, unchanged)
            if result.changed and not dry_run:
                ordered = sorted(rows, key=lambda row: row.action_id)
                table = pa.Table.from_pylist([asdict(row) for row in ordered], schema=self.schema)
                publish_parquet(path, table, sort_column="action_id")
            return result

    def apply_repairs(
        self,
        symbol: str,
        *,
        add_splits: list[SplitAddition],
        cancel_ex_dates: list[date],
        fetched_at: datetime,
        provider: str = "yahoo",
        dry_run: bool = False,
    ) -> RepairResult:
        """Add reference splits and cancel spurious active splits in one atomic mutation.

        Adds insert fresh active ``provider`` rows (revision 1). Cancels target the active
        split matching each ex-date and append a superseding ``cancelled`` revision — the
        lineage is retained, never deleted, mirroring ``reconcile``'s cancellation path.
        """
        symbol = canonical_symbol(symbol)
        path = self.path_for(symbol)
        lock = nullcontext() if dry_run else symbol_lock(path)
        with lock:
            rows = self._read(path)
            latest = self._latest_by_provider_id(rows)
            # ponytail: one active split per ex-date is assumed; splits colliding on a date
            # are vanishingly rare and reconcile never emits an add+cancel on the same date.
            active_split_by_exdate = {
                row.ex_date: row for row in latest.values() if row.action_type == "split" and row.status == "active"
            }
            added = cancelled = 0

            for addition in add_splits:
                event_id = f"{provider}|{symbol}|{addition.ex_date.isoformat()}|split"
                existing = latest.get(event_id)
                if existing is not None and existing.status == "active":
                    continue
                payload = f"{provider}|{symbol}|{addition.ex_date}|{addition.split_from}|{addition.split_to}"
                payload_hash = hashlib.blake2b(payload.encode(), digest_size=16).hexdigest()
                action = CorporateAction(
                    action_id=self._action_id(provider, event_id, 1, payload_hash),
                    provider=provider,
                    provider_event_id=event_id,
                    event_revision=1,
                    supersedes_action_id=None,
                    symbol=symbol,
                    action_type="split",
                    ex_date=addition.ex_date,
                    split_from=float(addition.split_from),
                    split_to=float(addition.split_to),
                    cash_amount=None,
                    currency=None,
                    declaration_date=None,
                    record_date=None,
                    pay_date=None,
                    status="active",
                    fetched_at=fetched_at,
                    payload_hash=payload_hash,
                )
                rows.append(action)
                latest[event_id] = action
                added += 1

            for ex_date in cancel_ex_dates:
                previous = active_split_by_exdate.get(ex_date)
                if previous is None:
                    continue
                cancelled_row = replace(
                    previous,
                    action_id=self._action_id(
                        previous.provider,
                        previous.provider_event_id,
                        previous.event_revision + 1,
                        previous.payload_hash,
                    ),
                    event_revision=previous.event_revision + 1,
                    supersedes_action_id=previous.action_id,
                    status="cancelled",
                    fetched_at=fetched_at,
                )
                rows.append(cancelled_row)
                latest[previous.provider_event_id] = cancelled_row
                cancelled += 1

            result = RepairResult(added, cancelled)
            if result.changed and not dry_run:
                ordered = sorted(rows, key=lambda row: row.action_id)
                table = pa.Table.from_pylist([asdict(row) for row in ordered], schema=self.schema)
                publish_parquet(path, table, sort_column="action_id")
            return result

    def latest_active(self, symbol: str) -> list[CorporateAction]:
        latest = self._latest_by_provider_id(self._read(self.path_for(canonical_symbol(symbol))))
        return sorted(
            (row for row in latest.values() if row.status == "active"),
            key=lambda row: (row.ex_date, row.action_type, row.action_id),
        )

    def history(self, symbol: str) -> list[CorporateAction]:
        return sorted(
            self._read(self.path_for(canonical_symbol(symbol))),
            key=lambda row: (row.provider_event_id, row.event_revision, row.action_id),
        )

    def _read(self, path: Path) -> list[CorporateAction]:
        if not path.exists():
            return []
        return [CorporateAction(**row) for row in pq.ParquetFile(path).read().to_pylist()]

    @staticmethod
    def _read_fetches(path: Path) -> list[CorporateActionFetch]:
        if not path.exists():
            return []
        return [CorporateActionFetch(**row) for row in pq.ParquetFile(path).read().to_pylist()]

    @staticmethod
    def _latest_by_provider_id(rows: list[CorporateAction]) -> dict[str, CorporateAction]:
        latest: dict[str, CorporateAction] = {}
        for row in rows:
            current = latest.get(row.provider_event_id)
            if current is None or row.event_revision > current.event_revision:
                latest[row.provider_event_id] = row
        return latest

    @classmethod
    def _from_provider(
        cls,
        event: ProviderEvent,
        fetched_at: datetime,
        *,
        revision: int,
        supersedes: str | None,
    ) -> CorporateAction:
        is_split = isinstance(event, MassiveSplit)
        return CorporateAction(
            action_id=cls._action_id(RECONCILE_PROVIDER, event.provider_event_id, revision, event.payload_hash),
            provider=RECONCILE_PROVIDER,
            provider_event_id=event.provider_event_id,
            event_revision=revision,
            supersedes_action_id=supersedes,
            symbol=event.ticker,
            action_type="split" if is_split else "cash_dividend",
            ex_date=event.execution_date if is_split else event.ex_dividend_date,
            split_from=float(event.split_from) if is_split else None,
            split_to=float(event.split_to) if is_split else None,
            cash_amount=None if is_split else float(event.cash_amount),
            currency=None if is_split else event.currency,
            declaration_date=None if is_split else event.declaration_date,
            record_date=None if is_split else event.record_date,
            pay_date=None if is_split else event.pay_date,
            status="active",
            fetched_at=fetched_at,
            payload_hash=event.payload_hash,
            source_ref=event.source_ref,
            source_hash=event.source_hash,
            source_fetched_at=event.source_fetched_at,
            source_cursor_identity=event.source_cursor_identity,
        )

    @staticmethod
    def _action_id(provider: str, event_id: str, revision: int, payload_hash: str) -> str:
        value = f"{provider}|{event_id}|{revision}|{payload_hash}".encode()
        return hashlib.blake2b(value, digest_size=16).hexdigest()
