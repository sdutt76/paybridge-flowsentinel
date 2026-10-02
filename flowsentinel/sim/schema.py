"""SQLite schema for one PayBridge simulator run.

Each run gets its own database file, ``data/runs/<run_id>.db`` (decision D7),
so scenarios never contaminate each other and every run can be inspected later.

Tables
------
topic_events      append-only log; ``id`` is the offset (D3)
consumer_offsets  committed position per (topic, consumer) (D3)
stage_telemetry   one row per stage per time window: counts and values (D4)
dead_letter       payments a stage could not process (D8)
logs              structured log lines emitted by stages
infra_state       infrastructure readings (lag, certificate days left, ...)
change_events     deploys and configuration changes
run_meta          facts about this run (run id, seed, schema version)

Run from the repo root:
    python -m flowsentinel.sim.schema                 # new run id
    python -m flowsentinel.sim.schema --run-id demo1  # chosen run id
"""

from __future__ import annotations

import argparse
import logging
import re
import secrets
import sqlite3
import sys
from datetime import datetime, timezone
from contextlib import closing
from pathlib import Path

LOGGER = logging.getLogger(__name__)

#: Bump when the table definitions change. Stored in SQLite's ``user_version``
#: so code can refuse to open a database built by an older schema.
SCHEMA_VERSION = 2

RUNS_DIR = Path("data") / "runs"

#: Run ids become file names, so they are restricted to safe characters.
#: This also blocks path tricks such as ``../../something``.
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

#: STRICT tables need SQLite 3.37 or later.
_MIN_SQLITE = (3, 37, 0)

SCHEMA = """
-- Append-only event log. The auto-increment id is the offset (D3).
-- uetr = business identity, copied unchanged by every stage (D8).
-- event_id = transport identity, new on every write (D8).
CREATE TABLE IF NOT EXISTS topic_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    topic            TEXT    NOT NULL,
    event_id         TEXT    NOT NULL,
    uetr             TEXT    NOT NULL,
    idempotency_key  TEXT,                 -- NULL only for malformed payments
    payload          TEXT    NOT NULL,     -- full payment JSON (contract v1)
    produced_at      TEXT    NOT NULL,     -- ISO 8601, UTC
    UNIQUE (event_id),
    UNIQUE (topic, uetr)                   -- stage-level idempotency (D8)
) STRICT;

-- Consumers read: WHERE topic = ? AND id > ? ORDER BY id
CREATE INDEX IF NOT EXISTS idx_topic_offset
    ON topic_events (topic, id);

-- End-to-end duplicate check: count(distinct idempotency_key) (D10)
CREATE INDEX IF NOT EXISTS idx_idempotency_key
    ON topic_events (topic, idempotency_key);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    topic        TEXT    NOT NULL,
    consumer     TEXT    NOT NULL,
    last_offset  INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT    NOT NULL,
    PRIMARY KEY (topic, consumer)
) STRICT;

-- One row per stage per window (D4). Outcomes differ by stage, so they are
-- stored as JSON, e.g. {"passed": 178, "rejected": 4, "dlq": 0}.
-- FX is the only stage where currency_in differs from currency_out (D10).
CREATE TABLE IF NOT EXISTS stage_telemetry (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    stage            TEXT    NOT NULL,
    window_start     TEXT    NOT NULL,
    window_end       TEXT    NOT NULL,
    records_in       INTEGER NOT NULL,
    outcomes_json    TEXT    NOT NULL,
    value_in_minor   INTEGER,
    value_out_minor  INTEGER,
    currency_in      TEXT,
    currency_out     TEXT,
    p95_latency_ms   REAL,
    recorded_at      TEXT    NOT NULL,
    UNIQUE (stage, window_start)           -- one row per stage per window
) STRICT;

CREATE TABLE IF NOT EXISTS dead_letter (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    stage      TEXT NOT NULL,
    uetr       TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    reason     TEXT NOT NULL,
    payload    TEXT NOT NULL,
    failed_at  TEXT NOT NULL,
    UNIQUE (stage, uetr)                   -- re-processing is a no-op (D8)
) STRICT;

CREATE TABLE IF NOT EXISTS logs (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       TEXT NOT NULL,
    stage    TEXT NOT NULL,
    level    TEXT NOT NULL,
    message  TEXT NOT NULL,
    uetr     TEXT                          -- set when the line is about one payment
) STRICT;

CREATE TABLE IF NOT EXISTS infra_state (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL,
    component  TEXT NOT NULL,              -- e.g. rail_gateway, ledger
    metric     TEXT NOT NULL,              -- e.g. cert_days_left, consumer_lag_s
    value      REAL NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS change_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    component    TEXT NOT NULL,
    change_type  TEXT NOT NULL,            -- deploy, config, reference_data
    description  TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS run_meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
) STRICT;
"""


class SchemaError(Exception):
    """Raised when the database cannot be created or is incompatible."""


def new_run_id() -> str:
    """Return a unique, file-safe run id such as ``20261002T074512Z-3f9a``.

    The timestamp makes runs sort in order; the random suffix keeps two runs
    started in the same second apart.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{secrets.token_hex(2)}"


def validate_run_id(run_id: str) -> str:
    """Return ``run_id`` unchanged if it is safe to use as a file name."""
    if not _RUN_ID_PATTERN.fullmatch(run_id):
        raise SchemaError(
            f"Invalid run id {run_id!r}: use 1-64 letters, digits, '-' or '_'."
        )
    return run_id


def run_db_path(run_id: str) -> Path:
    """Return the database path for a run: ``data/runs/<run_id>.db``."""
    return RUNS_DIR / f"{validate_run_id(run_id)}.db"


def _check_sqlite_version() -> None:
    if sqlite3.sqlite_version_info < _MIN_SQLITE:
        raise SchemaError(
            f"SQLite {sqlite3.sqlite_version} is too old; "
            f"{'.'.join(map(str, _MIN_SQLITE))}+ is needed for STRICT tables."
        )


def create_database(run_id: str, *, seed: int | None = None) -> Path:
    """Create (or open) the database for ``run_id`` and return its path.

    Safe to call twice for the same run: every statement uses IF NOT EXISTS,
    and run metadata is written with ON CONFLICT DO NOTHING.
    Refuses to touch a database built with a different schema version.
    """
    _check_sqlite_version()
    db_path = run_db_path(run_id)

    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(db_path)) as conn, conn:
            existing = conn.execute("PRAGMA user_version").fetchone()[0]
            if existing not in (0, SCHEMA_VERSION):
                raise SchemaError(
                    f"{db_path} uses schema version {existing}; this code "
                    f"expects {SCHEMA_VERSION}. Start a new run instead."
                )

            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

            meta = {
                "run_id": run_id,
                "schema_version": str(SCHEMA_VERSION),
                "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "seed": "" if seed is None else str(seed),
            }
            conn.executemany(
                "INSERT INTO run_meta (key, value) VALUES (?, ?) "
                "ON CONFLICT (key) DO NOTHING",
                meta.items(),
            )
    except sqlite3.Error as exc:
        raise SchemaError(f"Could not create database at {db_path}: {exc}") from exc

    LOGGER.info("Database ready: %s (schema v%d)", db_path, SCHEMA_VERSION)
    return db_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create the SQLite database for one simulator run."
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Run id (letters, digits, '-', '_'). Default: a new timestamped id.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    run_id = args.run_id or new_run_id()
    try:
        path = create_database(run_id)
    except SchemaError as exc:
        LOGGER.error("%s", exc)
        return 1
    print(f"run_id={run_id}")
    print(f"database={path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
