"""Initiate stage — the source of the PayBridge pipeline.

Simulates customers sending cross-border payments out of India over one day.
Every payment follows ``contracts/payment.v1.schema.json``, except a small,
deliberate share of malformed ones that the Validate stage must reject.

What this stage writes (all into ``data/runs/<run_id>.db``):
  * ``topic_events``    one row per payment on topic ``payments.initiated``
  * ``stage_telemetry`` one row per time window: how many were emitted, and
                        their total value in the source currency
  * ``run_meta``        run id and seed (via ``schema.create_database``)

And one file next to the database:
  * ``<run_id>.truth.json``  ground truth for this run (D6). A clean run says
                             "no failure". Agents never read this file.

Identity rules (D7, D8):
  * ``uetr``             business identity, minted here, copied by every stage
  * ``event_id``         transport identity, new on every write
  * ``end_to_end_id``    ``E2E-<run_id>-<seq>``, unique per run
  * ``idempotency_key``  derived from debtor account + end_to_end_id, never random

Money rules (D2): amounts are integers in minor units, built from integers
only. No float ever touches a stored amount.

Run from the repo root:
    python -m flowsentinel.sim.initiate --count 2000 --seed 42
    python -m flowsentinel.sim.initiate --count 2000 --start 2026-10-03T00:00+00:00 --run-id demo3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import sqlite3
import sys
import uuid
from collections import Counter
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from flowsentinel.sim.schema import (
    SchemaError,
    create_database,
    new_run_id,
    run_db_path,
    validate_run_id,
)

LOGGER = logging.getLogger(__name__)

PIPELINE_FILE = Path("pipeline.yaml")
STAGE = "initiate"
SCHEMA_VERSION = "1"            # payment contract version (D2), not the DB schema
SOURCE_CURRENCY = "INR"         # outbound remittances from India

#: Destination corridors: (destination currency, creditor country prefix, weight).
#: JPY (0 decimals) and KWD (3 decimals) are included on purpose, to exercise
#: the per-currency minor-unit exponents in pipeline.yaml.
CORRIDORS: tuple[tuple[str, str, float], ...] = (
    ("USD", "US", 0.40),
    ("GBP", "GB", 0.25),
    ("EUR", "DE", 0.20),
    ("JPY", "JP", 0.10),
    ("KWD", "KW", 0.05),
)
RAIL = "SWIFT"                  # all corridors here are cross-border

#: Relative traffic per UTC hour. India's business day (09:00-18:00 IST) is
#: roughly 03:30-12:30 UTC, so the peak sits there. Only the ratios matter.
HOURLY_WEIGHTS: dict[int, float] = {
    0: 0.3, 1: 0.4, 2: 0.8, 3: 1.6, 4: 2.2, 5: 2.5, 6: 2.4, 7: 2.2,
    8: 2.3, 9: 2.4, 10: 2.1, 11: 1.6, 12: 1.2, 13: 0.9, 14: 0.8, 15: 0.7,
    16: 0.6, 17: 0.5, 18: 0.4, 19: 0.3, 20: 0.3, 21: 0.2, 22: 0.2, 23: 0.2,
}

#: Amounts in whole rupees: log-normal (many small, few large), then clamped.
AMOUNT_MEDIAN_MAJOR = 25_000          # ₹25,000 typical payment
AMOUNT_SIGMA = 1.0                    # spread of the log-normal
AMOUNT_MIN_MAJOR = 100                # ₹100
AMOUNT_MAX_MAJOR = 5_000_000          # ₹50,00,000

#: Share of payments that are deliberately malformed (the Validate stage must
#: reject them). A probability per payment, not a quota.
INVALID_RATE = 0.02
#: Of the malformed ones: share missing the creditor account (the rest have a
#: zero amount). Two defect shapes, so Validate needs more than one rule.
DEFECT_MISSING_ACCOUNT_SHARE = 0.7

DEBTOR_POOL_SIZE = 500                # the same customers pay more than once
MAX_COUNT = 1_000_000
DEFAULT_WINDOW_SECONDS = 300

FIRST_NAMES = ("Aarav", "Diya", "Kabir", "Meera", "Rohan", "Isha", "Vikram",
               "Ananya", "Arjun", "Nisha", "Sameer", "Priya", "Dev", "Kavya")
LAST_NAMES = ("Sharma", "Iyer", "Menon", "Gupta", "Rao", "Nair", "Bose",
              "Kapoor", "Das", "Pillai", "Joshi", "Reddy")
CREDITOR_NAMES = ("J. Miller", "A. Schmidt", "K. Tanaka", "S. Brown",
                  "L. Fischer", "M. Sato", "R. Al-Sabah", "E. Clarke")


class InitiateError(Exception):
    """Raised for expected failures: bad arguments, bad config, database errors."""


@dataclass(frozen=True, slots=True)
class Party:
    name: str
    account: str | None


@dataclass(frozen=True, slots=True)
class Payment:
    """One payment event, as written to ``payments.initiated``."""

    event_id: str
    uetr: str
    end_to_end_id: str
    idempotency_key: str
    debtor: Party
    creditor: Party
    amount_minor: int
    currency: str
    dest_currency: str
    rail: str
    produced_at: datetime
    malformed: bool = field(default=False, compare=False)

    def payload(self) -> str:
        """The payment as contract-shaped JSON."""
        body: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "event_id": self.event_id,
            "uetr": self.uetr,
            "end_to_end_id": self.end_to_end_id,
            "idempotency_key": self.idempotency_key,
            "debtor": {"name": self.debtor.name, "account": self.debtor.account},
            "creditor": {"name": self.creditor.name, "account": self.creditor.account},
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "dest_currency": self.dest_currency,
            "rail": self.rail,
            "produced_at": self.produced_at.isoformat(),
        }
        return json.dumps(body, separators=(",", ":"))

    def to_row(self, topic: str) -> tuple[str, str, str, str, str, str]:
        return (
            topic,
            self.event_id,
            self.uetr,
            self.idempotency_key,
            self.payload(),
            self.produced_at.isoformat(),
        )


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

def load_pipeline(path: Path = PIPELINE_FILE) -> dict[str, Any]:
    """Load pipeline.yaml and check the parts this stage depends on."""
    try:
        with path.open(encoding="utf-8") as fh:
            config = yaml.safe_load(fh)
    except FileNotFoundError as exc:
        raise InitiateError(
            f"{path} not found. Run from the repo root (C:\\dev\\flowsentinel)."
        ) from exc
    except yaml.YAMLError as exc:
        raise InitiateError(f"{path} is not valid YAML: {exc}") from exc

    if not isinstance(config, dict):
        raise InitiateError(f"{path} must contain a mapping at the top level.")

    currencies = config.get("currencies", {})
    needed = {SOURCE_CURRENCY, *(c for c, _, _ in CORRIDORS)}
    missing = sorted(needed - set(currencies))
    if missing:
        raise InitiateError(f"Currencies missing from {path}: {', '.join(missing)}")

    stages = {s.get("name"): s for s in config.get("stages", [])}
    if STAGE not in stages or "writes" not in stages[STAGE]:
        raise InitiateError(f"Stage '{STAGE}' with a 'writes' topic not found in {path}.")
    return config


# --------------------------------------------------------------------------- #
# Building payments
# --------------------------------------------------------------------------- #

def derive_idempotency_key(debtor_account: str, end_to_end_id: str) -> str:
    """Derive the key from the payment itself (D2, D8): same inputs, same key."""
    material = f"{debtor_account}|{end_to_end_id}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()[:32]


def _account(rng: random.Random, country: str) -> str:
    """A synthetic account identifier. Not a real format; never a real account."""
    return f"{country}{rng.randint(10, 99)}PBRG{rng.randint(0, 10**10 - 1):010d}"


def build_debtor_pool(rng: random.Random, size: int = DEBTOR_POOL_SIZE) -> list[Party]:
    return [
        Party(f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}", _account(rng, "IN"))
        for _ in range(size)
    ]


def random_amount_minor(rng: random.Random, exponent: int) -> int:
    """A realistic amount, as an integer number of minor units.

    The size comes from a log-normal draw (a float), but it is turned into a
    whole number of rupees *before* it becomes money. Paise are added as an
    integer. So the stored amount never passes through float arithmetic.
    """
    major = int(rng.lognormvariate(0, AMOUNT_SIGMA) * AMOUNT_MEDIAN_MAJOR)
    major = max(AMOUNT_MIN_MAJOR, min(AMOUNT_MAX_MAJOR, major))
    minor_part = rng.randrange(10**exponent) if exponent else 0
    return major * 10**exponent + minor_part


def make_payment(
    rng: random.Random,
    *,
    run_id: str,
    seq: int,
    produced_at: datetime,
    debtors: list[Party],
    exponents: dict[str, int],
    malformed: bool = False,
) -> Payment:
    """Build one payment. ``malformed=True`` injects one contract defect."""
    if produced_at.tzinfo is None:
        raise InitiateError("produced_at must be timezone-aware (UTC).")

    dest_currency, country, _ = rng.choices(CORRIDORS, weights=[w for *_, w in CORRIDORS])[0]
    debtor = rng.choice(debtors)
    creditor = Party(rng.choice(CREDITOR_NAMES), _account(rng, country))
    amount = random_amount_minor(rng, exponents[SOURCE_CURRENCY])

    if malformed:
        if rng.random() < DEFECT_MISSING_ACCOUNT_SHARE:
            creditor = Party(creditor.name, None)        # defect 1: no creditor account
        else:
            amount = 0                                    # defect 2: non-positive amount

    end_to_end_id = f"E2E-{run_id}-{seq:06d}"
    return Payment(
        event_id=str(uuid.uuid4()),
        uetr=str(uuid.uuid4()),
        end_to_end_id=end_to_end_id,
        idempotency_key=derive_idempotency_key(debtor.account or "", end_to_end_id),
        debtor=debtor,
        creditor=creditor,
        amount_minor=amount,
        currency=SOURCE_CURRENCY,
        dest_currency=dest_currency,
        rail=RAIL,
        produced_at=produced_at,
        malformed=malformed,
    )


def _minute_weights(rng: random.Random, start: datetime, minutes: int) -> list[float]:
    """Weight for each minute: the hour's weight with ±15% jitter."""
    return [
        HOURLY_WEIGHTS.get((start + timedelta(minutes=m)).hour, 1.0) * rng.uniform(0.85, 1.15)
        for m in range(minutes)
    ]


def generate_day(
    rng: random.Random,
    *,
    run_id: str,
    start: datetime,
    count: int,
    exponents: dict[str, int],
) -> list[Payment]:
    """Spread about ``count`` payments over the 24 hours after ``start``."""
    if start.tzinfo is None:
        raise InitiateError("start must be timezone-aware.")
    if not 1 <= count <= MAX_COUNT:
        raise InitiateError(f"count must be between 1 and {MAX_COUNT:,}.")

    minutes = 24 * 60
    weights = _minute_weights(rng, start, minutes)
    total = sum(weights)
    debtors = build_debtor_pool(rng)

    payments: list[Payment] = []
    seq = 0
    for m, weight in enumerate(weights):
        exact = count * weight / total
        n = int(exact)
        if rng.random() < exact - n:          # probabilistic rounding
            n += 1
        minute_start = start + timedelta(minutes=m)
        for _ in range(n):
            seq += 1
            produced_at = minute_start + timedelta(seconds=rng.uniform(0, 60))
            payments.append(
                make_payment(
                    rng,
                    run_id=run_id,
                    seq=seq,
                    produced_at=produced_at,
                    debtors=debtors,
                    exponents=exponents,
                    malformed=rng.random() < INVALID_RATE,
                )
            )

    # Offsets must follow time (D3): sort before writing.
    payments.sort(key=lambda p: p.produced_at)
    LOGGER.info("Generated %d payments for run %s", len(payments), run_id)
    return payments


# --------------------------------------------------------------------------- #
# Telemetry
# --------------------------------------------------------------------------- #

def _window_start(ts: datetime, window_seconds: int) -> datetime:
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % window_seconds, tz=timezone.utc)


def build_telemetry(
    payments: list[Payment],
    *,
    start: datetime,
    window_seconds: int,
    recorded_at: datetime,
) -> list[tuple[Any, ...]]:
    """One row per window of the day: emitted count and value (source currency).

    Every window gets a row, including windows where nothing was emitted.
    A missing row and a row saying zero mean different things: zero is a
    measurement ("we looked, nothing came"), a missing row is an absence of
    data. Upstream silence can only be detected from explicit zeros.
    """
    counts: Counter[datetime] = Counter()
    values: Counter[datetime] = Counter()
    for p in payments:
        w = _window_start(p.produced_at, window_seconds)
        counts[w] += 1
        values[w] += p.amount_minor

    first = _window_start(start, window_seconds)
    n_windows = (24 * 3600) // window_seconds
    rows = []
    for i in range(n_windows):
        w = first + timedelta(seconds=i * window_seconds)
        rows.append((
            STAGE,
            w.isoformat(),
            (w + timedelta(seconds=window_seconds)).isoformat(),
            0,                                      # records_in: the source reads nothing
            json.dumps({"emitted": counts[w]}),
            None,                                   # value_in_minor
            values[w],                              # value_out_minor
            None,                                   # currency_in
            SOURCE_CURRENCY,                        # currency_out
            None,                                   # p95_latency_ms
            recorded_at.isoformat(timespec="seconds"),
        ))
    return rows


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #

def write_run(
    db_path: Path,
    *,
    topic: str,
    payments: list[Payment],
    telemetry: list[tuple[Any, ...]],
) -> int:
    """Write payments and telemetry in ONE transaction. Returns payments inserted.

    One transaction means a crash leaves either everything or nothing — never
    payments without their telemetry. ON CONFLICT DO NOTHING makes a retry of
    the same rows harmless (D8).
    """
    try:
        with closing(sqlite3.connect(db_path)) as conn, conn:
            cur = conn.executemany(
                "INSERT INTO topic_events "
                "(topic, event_id, uetr, idempotency_key, payload, produced_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT DO NOTHING",
                (p.to_row(topic) for p in payments),
            )
            inserted = cur.rowcount
            conn.executemany(
                "INSERT INTO stage_telemetry "
                "(stage, window_start, window_end, records_in, outcomes_json, "
                " value_in_minor, value_out_minor, currency_in, currency_out, "
                " p95_latency_ms, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (stage, window_start) DO NOTHING",
                telemetry,
            )
    except sqlite3.Error as exc:
        raise InitiateError(f"Could not write to {db_path}: {exc}") from exc
    return inserted

def write_ground_truth(
    db_path: Path,
    run_id: str,
    *,
    seed: int | None,
    requested_count: int,
    actual_count: int,
    malformed_count: int,
) -> Path:
    """Ground truth for evaluation (D6). Clean run: no failure injected.

    Records what actually happened, not just what was asked for:
    --count is a target, and probabilistic rounding makes the real total vary.
    """
    truth_path = db_path.with_suffix(".truth.json")
    truth = {
        "run_id": run_id,
        "seed": seed,
        "requested_count": requested_count,
        "actual_count": actual_count,
        "malformed_count": malformed_count,
        "failure": None,               # injectors arrive later in M1
    }
    truth_path.write_text(json.dumps(truth, indent=2), encoding="utf-8")
    return truth_path


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #

def _parse_start(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not ISO 8601, e.g. 2026-10-03T00:00+00:00"
        ) from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--start needs a timezone, e.g. +00:00")
    return parsed.astimezone(timezone.utc)


def _default_start() -> datetime:
    today = datetime.now(timezone.utc).date()
    return datetime(today.year, today.month, today.day, tzinfo=timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PayBridge Initiate stage: generate one day of payments.")
    parser.add_argument("--count", type=int, default=2000, help="Approximate number of payments (default 2000).")
    parser.add_argument("--start", type=_parse_start, default=None,
                        help="Start of the 24-hour day, ISO 8601 with timezone. Default: today 00:00 UTC.")
    parser.add_argument("--seed", type=int, default=None, help="Seed for reproducible runs.")
    parser.add_argument("--run-id", default=None, help="Run id. Default: a new timestamped id.")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    try:
        run_id = validate_run_id(args.run_id) if args.run_id else new_run_id()
        db_path = run_db_path(run_id)
        if db_path.exists():
            raise InitiateError(
                f"Run {run_id} already exists ({db_path}). Each run is generated once (D7); "
                "use a new --run-id."
            )

        config = load_pipeline()
        exponents = {code: int(exp) for code, exp in config["currencies"].items()}
        topic = next(s["writes"] for s in config["stages"] if s["name"] == STAGE)
        window_seconds = int(config.get("defaults", {}).get("telemetry_window_seconds", DEFAULT_WINDOW_SECONDS))

        rng = random.Random(args.seed)          # own generator, not the global one
        start = args.start or _default_start()

        create_database(run_id, seed=args.seed)
        payments = generate_day(rng, run_id=run_id, start=start, count=args.count, exponents=exponents)
        malformed = sum(p.malformed for p in payments)    # True counts as 1
        telemetry = build_telemetry(payments, start=start, window_seconds=window_seconds,
                                    recorded_at=datetime.now(timezone.utc))
        inserted = write_run(db_path, topic=topic, payments=payments, telemetry=telemetry)
        truth_path = write_ground_truth(
            db_path,
            run_id,
            seed=args.seed,
            requested_count=args.count,
            actual_count=len(payments),
            malformed_count=malformed,
        )
    except (InitiateError, SchemaError) as exc:
        LOGGER.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted.")
        return 130

    print(f"run_id={run_id}")
    print(f"database={db_path}")
    print(f"ground_truth={truth_path}")
    print(f"topic={topic} inserted={inserted} malformed={malformed} "
          f"telemetry_windows={len(telemetry)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
