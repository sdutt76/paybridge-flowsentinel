# FlowSentinel — Design Decisions

## D1. Stage names
validate → enrich → publish → gateway
Why: Each stage does one distinct thing to an event — checks it, adds reference data to it, converts it into the downstream format, then hands it over. Keeping them separate gives four points where a record can be counted and can go missing, so a failure can be pinned to one stage rather than to "the pipeline".

## D2. Event schema
event_id, correlation_id, comm_type, payload (json), produced_at (UTC ISO)
Why correlation_id separate from event_id: event_id identifies one attempt; correlation_id identifies the business message. When an event is retried or replayed it can get a new event_id, but it keeps the same correlation_id, so it can still be traced end to end and duplicates can be detected.
Why UTC everywhere: Stages are compared by time window. If any component records local time, the same window means different moments in different places, and gaps appear that are really clock differences. UTC makes every window mean the same instant everywhere.

## D3. Topics as SQLite tables with offsets
topic_events (id = offset), consumer_offsets (topic, consumer, last_offset)
Why not real Kafka: Kafka needs a JVM, a broker and a coordination service — more than a 4 GB laptop can run alongside everything else. The project is about observing a pipeline, not operating a broker.
What this preserves from Kafka's model: An append-only log, where each record has a position (the offset) and each consumer tracks its own last offset independently. So a consumer that crashes resumes where it stopped, several consumers can read the same topic without interfering, and lag is simply the latest offset minus the consumer's offset.

## D4. Telemetry row shape
run_id, stage, window_start, window_end, records_in, records_out, rejects, reject_reasons
Why rejects are separate from records_out: A record that doesn't come out of a stage is either a legitimate rejection (it failed validation, as designed) or a genuine loss. Counting rejects and their reasons separately means records_in = records_out + rejects for a healthy stage, and anything outside that equation is unexplained loss — which is what the agents look for.

## D5. Failure types (these become evaluation labels)
1. dependency_error   2. validation_spike   3. queue_lag
4. credential_expiry  5. upstream_silence
Why upstream_silence is the hardest to detect: Every other failure leaves evidence — an error in the logs, a gap between two stages, a growing queue. When the source simply stops sending, nothing fails and every stage balances perfectly at zero. It can only be caught by comparing arrivals against an expected volume for that time of day.

## D6. Ground truth
Every run writes ground_truth.json: failure_type, stage, window, affected_event_ids
Why this exists before any agent does: Without a known answer there is no way to measure whether an agent is right — only whether it produces a plausible-sounding response. Because the simulator injects each failure deliberately, it knows exactly what went wrong, and that becomes the label set the evaluation harness scores every agent finding against.