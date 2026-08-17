# SOLUTION.md

## What was broken, and why

**1. Duplicate rows and inflated stats.** `Ingest` decided whether an event
was new with `EventExists` (a `SELECT`) followed by a separate `InsertEvent`.
Nothing made those two statements atomic, and `events` had no uniqueness
constraint on `event_id` — only a plain index. Two redeliveries arriving
close together (which the provider explicitly does) could both see
`exists = false` before either had inserted, so both would insert, and both
would call `IncrementAccountStats`. That's exactly "duplicate call records"
and "call-counts drifting higher than actual." `TestDuplicateDeliveryIsIgnored`
missed it because it sends deliveries sequentially, never opening the race
window. Fixed in `store.IngestEvent`: a single `INSERT ... ON CONFLICT
(event_id) DO NOTHING` (backed by a new `UNIQUE` constraint,
`migrations/002_dedup_events.sql`) makes the dedup decision atomic — Postgres
serializes the conflict, not application code. The event insert, call
upsert, and stats increment now also run in one transaction, closing a
second, narrower gap: previously a failure between `InsertEvent` and
`IncrementAccountStats` would leave the event marked "seen" with the stats
increment lost forever, since a retry would then be (wrongly) treated as a
duplicate.

**2. Recordings never marked processed, silently.** `processRecording` ran
in a goroutine spawned inside `Ingest`, using the request's `context.Context`.
`net/http` cancels that context the instant the handler returns — which
happens right after the goroutine is spawned, well before the 50ms simulated
work finishes. So the later `MarkRecordingProcessed` call almost always ran
against an already-cancelled context and failed immediately. The error was
discarded (`// TODO: handle`), which is why nothing showed up in the logs.
Fixed by detaching the goroutine from the request's cancellation
(`context.WithoutCancel`) while still bounding it with its own timeout, and
by logging the error instead of dropping it.

**3. In-flight work disappearing on deploy.** The recording goroutine was
completely untracked. `srv.Shutdown` only waits for HTTP handlers still in
flight; it has no idea the handler spawned background work after returning.
`main` would return right after `Shutdown`, and the process would exit with
that goroutine mid-sleep. Fixed with a `sync.WaitGroup` on `Service`, plus a
new `Service.Wait(ctx)` that `main` calls after `srv.Shutdown` so the process
doesn't exit until pending recording work has actually finished (or a
deadline is hit).

## Why this deduplication strategy

I used Postgres (a `UNIQUE` constraint + `ON CONFLICT DO NOTHING` inside a
transaction) rather than Redis (e.g. `SETNX event:<id>`), for one reason:
Postgres is already the durable source of truth for this data, so putting
the correctness guarantee there means "inserted" and "committed" are the
same fact — no separate system to fall out of sync with. A Redis-based
lock is faster to check but doesn't own durability: if Redis restarts
without persistence, or the key expires, or the app crashes between
"claimed in Redis" and "written to Postgres," you're back to double-writes
— now silently, since the whole point of the Redis check was to skip the
DB. I'd trust a DB constraint I can't accidentally race around over an
external gate I have to keep consistent with the DB by hand. Application-level
locking (mutex per event_id) doesn't help at all here since redeliveries
usually arrive on different connections/replicas.

## At 10,000 webhooks/sec

The transaction-per-request round trip to Postgres becomes the bottleneck
well before that. I'd: batch inserts (buffer a small window of events and
`COPY`/multi-row insert them, with `account_stats` incremented via a
periodic aggregate job instead of one `UPDATE` per event); move the
uniqueness check to Redis (`SETNX` with a TTL) as a fast pre-filter to avoid
round-tripping to Postgres for the (common, low-cost) redelivery case, while
still treating the Postgres constraint as the final authority for anything
that gets past it; and shard `account_stats` writes by account or move them
off the hot path into a stream (the events table becomes the append-only log,
stats become a downstream consumer). I'd also revisit `processRecording`
running as one goroutine per request with no concurrency limit — at this
volume it needs a bounded worker pool or a real queue, not `go func()`.

## What I'd have done with more time

Load-test the concurrent-dedup fix under real contention (pgbench-style),
and add an index/partial-index check on `account_stats` writes at scale —
the current single-row `UPDATE ... SET x = x + 1` pattern is correct but
serializes all writes for one account, which will show up as lock
contention on high-volume accounts before 10k/sec is reached.
