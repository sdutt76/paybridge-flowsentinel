"""Print sanity checks for one simulator run.

    python -m flowsentinel.sim.inspect_run --run-id demo3

Read-only: opens the run's database and reports what is in it. Use it after
every run instead of typing SQL into the terminal.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from contextlib import closing

from flowsentinel.sim.schema import SchemaError, run_db_path

CHECKS: tuple[tuple[str, str], ...] = (
    ("events per topic",
     "SELECT topic, COUNT(*) FROM topic_events GROUP BY topic"),
    ("distinct uetr / idempotency keys",
     "SELECT COUNT(DISTINCT uetr), COUNT(DISTINCT idempotency_key) FROM topic_events"),
    ("amount stored as",
     "SELECT DISTINCT typeof(json_extract(payload, '$.amount_minor')) FROM topic_events"),
    ("malformed (no creditor account / zero amount)",
     "SELECT SUM(json_extract(payload, '$.creditor.account') IS NULL), "
     "       SUM(json_extract(payload, '$.amount_minor') = 0) FROM topic_events"),
    ("payments per destination currency",
     "SELECT json_extract(payload, '$.dest_currency'), COUNT(*) FROM topic_events "
     "GROUP BY 1 ORDER BY 2 DESC"),
    ("busiest UTC hours",
     "SELECT substr(produced_at, 12, 2), COUNT(*) FROM topic_events "
     "GROUP BY 1 ORDER BY 2 DESC LIMIT 3"),
    ("offsets out of time order (must be 0)",
     "SELECT COUNT(*) FROM (SELECT produced_at, "
     "LAG(produced_at) OVER (ORDER BY id) AS prev FROM topic_events) "
     "WHERE prev > produced_at"),
    ("telemetry windows / windows with zero / emitted total",
     "SELECT COUNT(*), SUM(json_extract(outcomes_json, '$.emitted') = 0), "
     "SUM(json_extract(outcomes_json, '$.emitted')) FROM stage_telemetry "
     "WHERE stage = 'initiate'"),
    ("value: telemetry total vs payload total (must match)",
     "SELECT (SELECT SUM(value_out_minor) FROM stage_telemetry WHERE stage = 'initiate'), "
     "(SELECT SUM(json_extract(payload, '$.amount_minor')) FROM topic_events "
     " WHERE topic = 'payments.initiated')"),
    ("run_meta",
     "SELECT key, value FROM run_meta ORDER BY key"),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sanity checks for one simulator run.")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    try:
        db_path = run_db_path(args.run_id)
    except SchemaError as exc:
        print(f"ERROR {exc}")
        return 1
    if not db_path.exists():
        print(f"ERROR no database at {db_path}")
        return 1

    # mode=ro: open read-only, so this tool can never change a run.
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        for title, sql in CHECKS:
            rows = conn.execute(sql).fetchall()
            print(f"\n{title}")
            for row in rows:
                print("   ", *row)
    return 0


if __name__ == "__main__":
    sys.exit(main())
