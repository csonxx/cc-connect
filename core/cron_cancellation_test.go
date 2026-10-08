package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type deadlineAgent struct {
	stubAgent
	started chan struct{}
	calls   atomic.Int32
	session *deadlineSession
}

func (a *deadlineAgent) StartSession(ctx context.Context, _ string) (AgentSession, error) {
	a.calls.Add(1)
	select {
	case a.started <- struct{}{}:
	default:
	}
	if a.session == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return a.session, nil
}

type deadlineSession struct {
	stubAgentSession
	events chan Event
	sent   chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (s *deadlineSession) Send(_ string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	select {
	case s.sent <- struct{}{}:
	default:
	}
	return nil
}
func (s *deadlineSession) Events() <-chan Event { return s.events }
func (s *deadlineSession) Close() error {
	s.once.Do(func() { close(s.closed); close(s.events) })
	return nil
}
func (s *deadlineSession) Alive() bool {
	select {
	case <-s.closed:
		return false
	default:
		return true
	}
}
func cronDeadlineEngine(t *testing.T, a Agent) *Engine {
	t.Helper()
	p := &stubCronReplyTargetPlatform{stubPlatformEngine: stubPlatformEngine{n: "discord"}}
	e := NewEngine("test", a, []Platform{p}, "", LangEnglish)
	t.Cleanup(e.cancel)
	return e
}
func TestCronDeadlineCancelsSessionStartup(t *testing.T) {
	a := &deadlineAgent{started: make(chan struct{}, 1)}
	e := cronDeadlineEngine(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := e.ExecuteCronJobContext(ctx, &CronJob{ID: "slow-start", SessionKey: "discord:channel:user", Prompt: "work", Mute: true, SessionMode: "new_per_run"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want actual deadline", err)
	}
	if a.calls.Load() != 1 {
		t.Fatalf("starts = %d", a.calls.Load())
	}
}
func TestCronDeadlineClosesRunningSession(t *testing.T) {
	s := &deadlineSession{events: make(chan Event), sent: make(chan struct{}, 1), closed: make(chan struct{})}
	a := &deadlineAgent{started: make(chan struct{}, 1), session: s}
	e := cronDeadlineEngine(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := e.ExecuteCronJobContext(ctx, &CronJob{ID: "running", SessionKey: "discord:channel:user", Prompt: "work", Mute: true, SessionMode: "new_per_run"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	select {
	case <-s.sent:
	default:
		t.Fatal("task never reached the agent")
	}
	select {
	case <-s.closed:
	case <-time.After(time.Second):
		t.Fatal("deadline did not close actual session")
	}
	if s.Alive() {
		t.Fatal("agent still alive")
	}
}
func TestCronManualWakeCannotOverlapAndReleasesGuard(t *testing.T) {
	a := &deadlineAgent{started: make(chan struct{}, 1)}
	e := cronDeadlineEngine(t, a)
	store, err := NewCronStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scheduler := NewCronScheduler(store)
	scheduler.RegisterEngine("test", e)
	noLimit := 0
	job := &CronJob{ID: "overlap", Project: "test", SessionKey: "discord:channel:user", Prompt: "work", Mute: true, SessionMode: "new_per_run", TimeoutMins: &noLimit}
	if err := store.Add(job); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { scheduler.runJob(job, true); close(done) }()
	select {
	case <-a.started:
	case <-time.After(time.Second):
		t.Fatal("not started")
	}
	scheduler.runJob(job, true)
	if a.calls.Load() != 1 {
		t.Fatalf("overlapping starts = %d", a.calls.Load())
	}
	e.cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancel did not finish")
	}
	if _, ok := scheduler.runningJobs.Load("test:overlap"); ok {
		t.Fatal("execution guard leaked")
	}
	if store.Get(job.ID).LastError == "" {
		t.Fatal("cancel incorrectly recorded success")
	}
}
