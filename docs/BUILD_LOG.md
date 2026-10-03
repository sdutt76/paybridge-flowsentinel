# Build log — PayBridge + FlowSentinel

One entry per session, appended at the bottom. Format: what was done · decisions · next.

---

**21 Sep 2026** — Python 3.13, virtual environment, Git and GitHub set up; repo created.

**22 Sep 2026** — Package layout; first decision log (D1–D6, commit `8af5624`); `schema.py` with 7 tables, WAL mode (commit `a541dc8`).

**23–26 Sep 2026** — Event generator (`generator.py`): hourly weights, jitter, probabilistic rounding, seeded runs. Worked through seeds, UUIDs and idempotency; drafted D7 (re-runs) and D8 (identity at the edge).

**27–28 Sep 2026** — Pivoted to PayBridge + FlowSentinel (blueprint v2.0, then v2.1: per-leg FX reconciliation, rail-vs-ledger detector, 5 injectors in M1, S2 trimmed).

**01 Oct 2026** — Payment contract v1 (`contracts/`), `pipeline.yaml` (7 stages, end-to-end checks, 10 failure types), decision log rev 2 (D1–D14), README rewritten.
Decisions: one database per run plus a soak run in M5; `end_to_end_id` includes the run id.
Open: `additionalProperties: false` vs forward compatibility; ADR 0003 draft.
Next: `schema.py` for payments, per-run database, generator → Initiate stage.


**02 Oct 2026** — requirements.txt (pyyaml 6.0.3); schema v2: one database per run, uetr identity, STRICT tables, telemetry per D4, dead letter with reasons, run_meta, user_version check, contextlib.closing.
   Decisions: D4 refined (currency_in / currency_out).
   Next: generator → initiate.py.

**03 Oct 2026** — Initiate stage (`initiate.py`): 1,975 payments per contract v1 (seed 42), integer money from integers only, derived idempotency keys, `E2E-<run_id>-<seq>`; 288 telemetry windows including explicit zeros; ground truth records requested vs actual counts; reusing a run id is refused (D7 in code). `inspect_run.py` read-only checks tool. First reconciliation passed: telemetry value = payload value (7,853,359,055 minor units).
Fixed: virtual environment not active in terminal (check `sys.executable`).
Next: Validate stage — consumer pattern, offsets, rejections with reasons, DLQ, telemetry; expect in 1,975 = passed 1,934 + rejected 41.