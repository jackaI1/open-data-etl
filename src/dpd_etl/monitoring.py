"""Opt-in, aggregate-only Datadog metrics for the driver, never Spark workers."""

from __future__ import annotations

import logging
import os
import re
from contextlib import contextmanager
from time import perf_counter
from typing import TYPE_CHECKING

from datadog import DogStatsd

if TYPE_CHECKING:
    from .quality import Check

LOG = logging.getLogger(__name__)
CHECK_NAMES = frozenset({
    "row_count_min", "raw_vs_deduped_rows", "drug_code_unique", "null_ratio_brand_name",
    "null_ratio_din", "null_ratio_last_update_date", "null_ratio_company_name",
    "null_ratio_current_status", "orphan_ratio_status", "orphan_ratio_ingredients",
    "null_ratio_status_valid_from", "scd2_single_current_version", "scd2_valid_intervals",
    "row_drop_vs_previous_snapshot",
})


class Monitor:
    def __init__(self, client: DogStatsd | None = None):
        self.client = client
        self.warned = False

    @classmethod
    def from_env(cls) -> Monitor:
        enabled = os.environ.get("DPD_DATADOG_ENABLED", "false").lower()
        if enabled not in {"true", "false"}:
            raise ValueError("DPD_DATADOG_ENABLED must be true or false")
        if enabled == "false":
            return cls()
        if os.environ.get("DD_EXTERNAL_ENV"):
            raise ValueError("Unset DD_EXTERNAL_ENV to keep monitoring aggregate-only")
        tags = ["service:open-data-etl", "dataset:marketed-dpd"]
        for key, default in [("env", "local"), ("version", "dev")]:
            value = os.environ.get(f"DD_{key.upper()}", default)
            if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", value):
                raise ValueError(f"DD_{key.upper()} must be a short lowercase label")
            tags.append(f"{key}:{value}")
        try:
            port = int(os.environ.get("DD_DOGSTATSD_PORT", "8125"))
        except ValueError:
            raise ValueError("DD_DOGSTATSD_PORT must be an integer from 1 to 65535") from None
        if not 1 <= port <= 65535:
            raise ValueError("DD_DOGSTATSD_PORT must be an integer from 1 to 65535")
        client = DogStatsd(
            host=os.environ.get("DD_AGENT_HOST", "127.0.0.1"),
            port=port,
            namespace="dpd_etl",
            disable_buffering=True,
            disable_background_sender=True,
            disable_telemetry=True,
            origin_detection_enabled=False,
            socket_timeout=0,
        )
        # The library normally appends DATADOG_TAGS, DD_SERVICE and DD_ENTITY_ID.
        # Replace that list rather than letting arbitrary environment tags escape.
        client.constant_tags = tags
        return cls(client)

    def _emit(self, method: str, *args, **kwargs) -> None:
        if self.client is None:
            return
        try:
            getattr(self.client, method)(*args, **kwargs)
        except Exception:  # noqa: BLE001 - isolate all optional telemetry failures
            # Monitoring must not hide a quality/load failure or stop a good run.
            # Omit exception text: it can contain an endpoint or other private data.
            if not self.warned:
                LOG.warning("Datadog metrics unavailable; pipeline result is unchanged")
                self.warned = True

    def quality(self, checks: list[Check]) -> None:
        for check in checks:
            if check.name in CHECK_NAMES:
                self._emit("gauge", "quality.observed", check.observed,
                           tags=[f"check:{check.name}"])
                self._emit("gauge", "quality.failed", int(not check.passed),
                           tags=[f"check:{check.name}"])
        self._emit("service_check", "dpd_etl.quality_gate", 0 if all(c.passed for c in checks) else 2)

    def products(self, count: int) -> None:
        self._emit("gauge", "products", count)

    @contextmanager
    def command(self, command: str):
        from .quality import DataQualityError

        if command not in {"extract", "transform", "delta"}:
            raise ValueError("unknown monitored command")
        started = perf_counter()
        outcome = "success"
        try:
            yield self
        except DataQualityError:
            outcome = "quality_failed"
            raise
        except BaseException:
            outcome = "error"
            raise
        finally:
            tags = [f"command:{command}", f"outcome:{outcome}"]
            self._emit("increment", "runs", tags=tags)
            self._emit("timing", "duration_ms", (perf_counter() - started) * 1000, tags=tags)
            self._emit("service_check", "dpd_etl.run", 0 if outcome == "success" else 2,
                       tags=[f"command:{command}"])
            # Synchronous, unbuffered sends: no flush threads or queues to leave behind.
            self._emit("close_socket")
