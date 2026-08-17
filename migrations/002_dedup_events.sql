-- The provider redelivers events (same event_id) at least once. The service
-- used to decide "have I seen this?" with a SELECT followed by a separate
-- INSERT, which is not atomic: two redeliveries arriving close together can
-- both pass the SELECT before either INSERT lands, producing duplicate rows
-- in `events` and double-incrementing `account_stats`.
--
-- A UNIQUE constraint moves that decision into a single statement
-- (`INSERT ... ON CONFLICT (event_id) DO NOTHING`), so Postgres itself
-- serializes concurrent redeliveries and only one can ever "win".
DROP INDEX IF EXISTS idx_events_event_id;

ALTER TABLE events
    ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
