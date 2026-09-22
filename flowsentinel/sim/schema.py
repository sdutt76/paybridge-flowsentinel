"""Creates the SQLite database the simulator writes to.

Each table stands in for a real system:
  topic_events      -> Kafka topics (the auto-increment id is the offset)
  consumer_offsets  -> Kafka consumer groups
  stage_telemetry   -> per-stage monitoring counts
  logs              -> application logs
  infra_state       -> infrastructure metrics
  change_events     -> deployment and maintenance records
  dead_letter       -> records queued for retry after a recoverable failure
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("data") / "flowsentinel.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS topic_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,   -- the offset
    topic           TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    correlation_id  TEXT NOT NULL,
    comm_type       TEXT NOT NULL,
    payload         TEXT,
    produced_at     TEXT NOT NULL                        -- UTC ISO-8601
);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    topic           TEXT NOT NULL,
    consumer        TEXT NOT NULL,
    last_offset     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, consumer)
);

CREATE TABLE IF NOT EXISTS stage_telemetry (
    run_id          TEXT NOT NULL,
    stage           TEXT NOT NULL,
    window_start    TEXT NOT NULL,
    window_end      TEXT NOT NULL,
    records_in      INTEGER NOT NULL,
    records_out     INTEGER NOT NULL,
    rejects         INTEGER NOT NULL,
    reject_reasons  TEXT
);

CREATE TABLE IF NOT EXISTS logs (
    run_id          TEXT NOT NULL,
    ts              TEXT NOT NULL,
    stage           TEXT NOT NULL,
    level           TEXT NOT NULL,
    error_code      TEXT,
    message         TEXT
);

CREATE TABLE IF NOT EXISTS infra_state (
    run_id          TEXT NOT NULL,
    ts              TEXT NOT NULL,
    component       TEXT NOT NULL,
    metric          TEXT NOT NULL,
    value           REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS change_events (
    run_id          TEXT NOT NULL,
    ts              TEXT NOT NULL,
    type            TEXT NOT NULL,
    component       TEXT NOT NULL,
    description     TEXT
);

CREATE TABLE IF NOT EXISTS dead_letter (
    topic           TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    ts              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_topic_offset ON topic_events (topic, id);
"""


def create_database(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")  # lets agents read while the simulator writes
        conn.executescript(SCHEMA)
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    print(f"Database ready at {db_path}")
    for t in tables:
        print(f"  - {t}")


if __name__ == "__main__":
    create_database()