// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// recordingTimeout bounds how long background recording processing is
// allowed to run. It is independent of the inbound request's lifetime: the
// request context is cancelled the moment the HTTP handler returns, well
// before this background work is done.
const recordingTimeout = 5 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// wg tracks recording-processing goroutines spawned by Ingest so Wait
	// can block shutdown until they've all finished, instead of letting the
	// process exit out from under them.
	wg sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// IngestEvent makes the store/upsert/increment atomic and idempotent:
	// redeliveries of the same event_id are detected by a DB-level UNIQUE
	// constraint rather than a check-then-insert race, so a redelivery can
	// never double-write the call row or double-count the account totals.
	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the
	// provider. It must not inherit the request's context: net/http cancels
	// that context the moment this handler returns, which is right after
	// this goroutine is spawned - so processRecording would almost always
	// see an already-cancelled context and fail immediately. We detach from
	// cancellation but keep a bounded timeout, and we track the goroutine
	// with wg so a graceful shutdown can wait for it instead of killing it
	// mid-flight.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			bgCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordingTimeout)
			defer cancel()
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("process recording failed",
					"event_id", rec.EventID, "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// Wait blocks until every in-flight background goroutine spawned by Ingest
// has finished, or ctx is done - whichever comes first. Call it during
// graceful shutdown, after the HTTP server has stopped accepting new work,
// so in-flight recording processing isn't dropped when the process exits.
func (s *Service) Wait(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.log.Warn("shutdown deadline reached with recording work still in flight")
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
