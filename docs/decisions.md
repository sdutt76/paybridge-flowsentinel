# Decision log — PayBridge + FlowSentinel

Running log of design decisions, newest revision first within each entry.
Big decisions are also written up as full ADRs in `docs/adr/` (blueprint v2.1, Section 13).

Format: **Status** · **Date** · Context · Decision · Consequences.
Statuses: `Accepted`, `Proposed` (decide at the named phase), `Superseded` (kept for history, never deleted).

Revision 2 — 01 Oct 2026 — rewritten for blueprint v2.1 (PayBridge payments domain).
Revision 1 (communications pipeline) — original text is in git history, commit `8af5624`.

---

## D1 — Pipeline stages

**Accepted** · 01 Oct 2026 · supersedes D1 rev 1

Context: the use case moved from a 4-stage communications pipeline to the PayBridge real-time cross-border payment pipeline.

Decision: seven stages, each reading one topic and writing the next.

| Stage | Reads | Writes | Owner |
|---|---|---|---|
| initiate | — | payments.initiated | payments-ops |
| validate | payments.initiated | payments.validated | payments-ops |
| screen | payments.validated | payments.screened | compliance-ops |
| fx | payments.screened | payments.priced | treasury-ops |
| route | payments.priced | payments.routed | payments-ops |
| ledger | payments.routed | payments.posted | ledger-ops |
| notify | payments.posted | payments.notified | payments-ops |

Consequences: `pipeline.yaml` is the source of truth for this table (D9). Topology is inferred from `reads`/`writes` pairs, never hard-coded.

---

## D2 — Event contract

**Accepted** · 01 Oct 2026 · supersedes D2 rev 1

Context: payments need identity, money and parties, not a free-form comms payload.

Decision:
- Contract: `contracts/payment.v1.schema.json` (JSON Schema 2020-12), with a sample in `contracts/examples/payment.v1.example.json`.
- Simplified pacs.008 shape: `event_id`, `uetr`, `end_to_end_id`, `idempotency_key`, `debtor`, `creditor`, `amount_minor`, `currency`, `dest_currency`, `rail`, `produced_at`, `schema_version`.
- **Money is an integer in minor units. Never a float.** The decimal exponent per currency (INR 2, JPY 0, KWD 3) lives in `pipeline.yaml`.
- Timestamps are UTC and timezone-aware. Naive datetimes are rejected.
- The version lives in the filename. A breaking change creates `payment.v2.schema.json`; v1 stays readable.
- `debtor` and `creditor` are PII and are masked before any model call (M3).

Consequences: the contract sits at the repo root, not inside the Python package, because the Java services in S1 (Scale profile, 2027) must read the same file.

---

## D3 — Topics as SQLite tables

**Accepted** · 21 Sep 2026 · carried forward unchanged in principle

Context: zero spend and 4 GB RAM rule out running Kafka in 2026.

Decision:
- `topic_events` is an append-only log; the auto-increment `id` is the offset.
- `consumer_offsets (topic, consumer, last_offset)` stores committed positions.
- Consumers read `WHERE topic = ? AND id > ? ORDER BY id`.
- WAL journal mode; composite index on `(topic, id)`.
- Topic names follow D1 (`payments.*`).

Consequences: we keep ordering, replay, offsets and lag. We lose partitions, replication, retention and consumer-group rebalancing. Full write-up: ADR 03. Replaced by Redpanda in S1 (Scale profile, 2027) behind the `EventSource` port.

---

## D4 — Telemetry shape

**Accepted** · 01 Oct 2026 · supersedes D4 rev 1 (counts only)

Context: payment reconciliation needs values, not just counts.

Decision: one `stage_telemetry` row per stage per time window, holding:
- `stage`, `window_start`, `window_end` (UTC)
- `records_in` and one count per outcome named in `pipeline.yaml` (e.g. passed / rejected / dlq)
- `value_in_minor`, `value_out_minor` and `currency` — the currency is the source currency up to FX and the destination currency after FX
- `p95_latency_ms` where a stage has a latency SLA (FX)

Consequences: `schema.py` needs value columns (Day 2). Values are never summed across currencies (D10).

---

## D5 — Failure types

**Accepted** · 01 Oct 2026 · supersedes D5 rev 1 (five comms failure types)

Decision: ten injectable failure types plus clean runs, declared in `pipeline.yaml`:

| Built in M1 (payment-specific first) | Added in M5 (as evaluation scenarios) |
|---|---|
| upstream_silence | dependency_throttle |
| validation_spike | certificate_expiry |
| consumer_lag | poison_message |
| balance_break (rail vs ledger) | bad_deploy |
| duplicate_payments | prompt_injection |

Injectors are configuration-driven: type, stage, window, severity.

Consequences: the stage where a failure is injected is often not where it is detected (silence: injected at initiate, seen at validate). That gap is what the RCA agent must reason across.

---

## D6 — Ground truth

**Accepted** · 21 Sep 2026 · carried forward

Decision: every simulator run writes a ground-truth file: `run_id`, failure type, injected stage, time window, severity and affected `uetr`s. Clean runs write a file that says "no failure".

Consequences: ground truth is for evaluation only. **Agents never read it.** If ground truth leaks into anything an agent can see, every evaluation score becomes meaningless.

---

## D7 — Run isolation and simulator identity

**Accepted** · 01 Oct 2026 · supersedes D7 of 26 Sep

Context: every evaluation scenario must start from a known state, or leftover
events from an earlier run contaminate its windows and its scores. Separately,
the idempotency key is derived from seeded values, so two runs with the same
seed would produce identical keys.

Decision:
- One database per run: `data/runs/<run_id>.db`. Nothing is deleted; runs are
  isolated by construction and kept for inspection when a diagnosis is wrong.
- `end_to_end_id` includes the run: `E2E-<run_id>-<seq>`. Only the
  duplicate_payments injector reuses a key, on purpose.
- `--seed` reproduces counts, distribution, amounts, parties and injected
  failures; it does not reproduce `event_id` or `uetr` (uuid4 ignores the seed).
- Production-like behaviour is tested separately by a continuous soak run (M5):
  one database, no resets, overlapping and recurring failures.

Consequences: isolated scenario runs measure diagnostic accuracy; the soak run
measures behaviour over time (re-raising, recovery, overlap). Both are reported.
A production producer would derive `uetr` via `uuid5`; deferred.

---

## D8 — Identity at the edge

**Accepted** · 01 Oct 2026 · supersedes D8 of 26 Sep (correlation_id)

Decision:
- `uetr` is the **business identity**. It is minted once at `initiate` and copied unchanged by every stage. It replaces `correlation_id`.
- `event_id` is the **transport identity**: a new one on every write.
- `idempotency_key` is **derived**, never random (D2, D7).
- Stage-level idempotency: `UNIQUE (topic, uetr)` on `topic_events` and `UNIQUE (stage, uetr)` on `dead_letter`. Stages write with `INSERT ... ON CONFLICT DO NOTHING`, never `INSERT OR IGNORE`.
- Rail confirmations are matched to ledger postings one-to-one by `uetr` (D10).

Assumption: one payment produces at most one event per topic.
Revisit trigger: if a stage emits several events for the same payment (for example, several pacs.002 status updates), the key becomes composite, e.g. `(topic, uetr, status)`.

Consequences: `schema.py` changes `correlation_id` to `uetr` and adds the unique indexes (Day 2). Full write-up: ADR 16.

---

## D9 — Configuration over code

**Accepted** · 28 Sep 2026

Decision: `pipeline.yaml` at the repo root declares stages, topics, outcomes, balance rules, tolerances, SLAs, dependencies, owners and failure types. The simulator (M1), context graph (M2) and rules gate (M4) all read it.

- Balance rules are a small DSL. They are evaluated by a **restricted parser, never Python `eval()`**, because `eval()` executes any text as code.
- Rules may only reference counters declared in the stage's `outcomes`.

Consequences: a second domain (card authorisation or an ACH file, S2) onboards by writing a new YAML file. If that needs a code change, the principle has failed, and the failure is recorded here.

---

## D10 — Money reconciliation

**Accepted** · 28 Sep 2026

Context: cross-border payments change currency at FX, and a double-entry ledger enforces debits = credits on every posting.

Decision:
- **Value is reconciled per currency leg.** Source currency from initiate to fx; destination currency from fx to ledger. Totals are never compared across currencies.
- FX checks each payment: `source = dest / applied_rate + fee`, allowing ±1 minor unit for rounding. This is the only place where money isn't exact.
- The balance-break detector is **external reconciliation**: value posted = value sent on the rail, and rail confirmations match postings one-to-one.
- `sum(debits) == sum(credits)` is a ledger **constraint**, not a detector. It can't break by construction, so watching it would detect nothing.

Consequences: full write-up: ADR 15.

---

## D11 — Tolerances

**Accepted** · 28 Sep 2026

Decision:
- Zero tolerance **only for money and duplicates**.
- Everything else uses a band around a baseline (rejection 1–4%, held 0.5–3%, route failures 0–2%), because real rails and sources fail normally. Zero tolerance there would raise incidents on clean runs and wreck the suppression-rate metric.
- Balance checks run only after the settling window closes (300 s by default), so in-flight payments aren't reported as lost.

Consequences: the starting values in `pipeline.yaml` are guesses. They are re-baselined after the first full simulator run, and each change is logged here.

---

## D12 — Embedded graph after Kùzu's archival

**Proposed** · decide on M2 day 1

Context: Kùzu was archived by its sponsor in October 2025.

Options:
1. Pin the last Kùzu release or a community fork, **after** confirming a Windows wheel exists for Python 3.13.
2. SQLite with recursive CTEs.

Either option sits behind the `GraphStore` port; the Neo4j adapter follows in S1 (Scale profile, 2027).

Consequences: full write-up: ADR 04, a good interview example of dependency judgement.

---

## D13 — Classification behind a decision port

**Proposed** · implement in M4, evaluate in S1 (Scale profile, 2027)

Context: typed decision models (e.g. Jev, launched September 2026, early access) are positioned for routing and classification.

Decision:
- In M4, implement Classify behind `DecisionPort.decide(state, options) -> Decision(choice, confidence)`, using Gemini structured output.
- In S1 (Scale profile, 2027), add a typed-decision-model adapter and compare both on the evaluation set: category accuracy, calibration, p95 latency, cost per correct decision, and resistance to the prompt-injection scenario.
- Never use it for money checks (rules), the RCA explanation (an LLM), or approval (a human).

Consequences: no new dependency or spend in the MVP. Outcome recorded as ADR 17.

---

## D14 — Naming and separation

**Accepted** · 28 Sep 2026

Decision: nothing in this repository identifies a client, employer, platform or product. There is no communications or healthcare flavour anywhere, in PayBridge or in the second domain. The repo is built only from public knowledge, and the commit email is the GitHub noreply address.

Consequences: the old comms event types are removed when the generator becomes the initiate stage (Day 2).

---

## Superseded (revision 1, communications pipeline)

Kept for history. The full original text is in git at commit `8af5624`.

- **D1 rev 1**: four communications stages. Replaced by the seven PayBridge stages.
- **D2 rev 1**: comms event (`event_id`, `correlation_id`, `comm_type`, JSON `payload`, `produced_at`). Replaced by the payment contract.
- **D4 rev 1**: telemetry with counts only. Values added.
- **D5 rev 1**: five comms failure types. Replaced by ten payment failure types.
- **D7 of 26 Sep**: generator not idempotent; database deleted between runs. Replaced by one database per run (D7 rev 2). 
- **D8 of 26 Sep**: `correlation_id` as business identity. Renamed to `uetr`.
