package runner

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLogBroadcasterWrite(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	// Write a line
	n, err := b.Write([]byte("hello world\n"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 12 {
		t.Errorf("Write returned %d, want 12", n)
	}

	// Read with timeout
	select {
	case line := <-ch:
		if line != "hello world\n" {
			t.Errorf("got %q, want %q", line, "hello world\n")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for log line")
	}
}

func TestLogBroadcasterMultipleSubscribers(t *testing.T) {
	b := NewLogBroadcaster()
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()
	ch3 := b.Subscribe()

	_, _ = b.Write([]byte("test message\n"))

	// All subscribers should receive the message
	for i, ch := range []chan string{ch1, ch2, ch3} {
		select {
		case line := <-ch:
			if line != "test message\n" {
				t.Errorf("subscriber %d: got %q, want %q", i, line, "test message\n")
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("subscriber %d: timeout", i)
		}
	}
}

func TestLogBroadcasterUnsubscribe(t *testing.T) {
	b := NewLogBroadcaster()
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()

	// Unsubscribe ch1
	b.Unsubscribe(ch1)

	// ch1 should be closed
	select {
	case _, ok := <-ch1:
		if ok {
			t.Error("ch1 should be closed")
		}
	default:
		// Channel is closed but empty, this is fine
	}

	// ch2 should still work
	_, _ = b.Write([]byte("after unsubscribe\n"))

	select {
	case line := <-ch2:
		if line != "after unsubscribe\n" {
			t.Errorf("got %q, want %q", line, "after unsubscribe\n")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for log line on ch2")
	}
}

func TestLogBroadcasterClose(t *testing.T) {
	b := NewLogBroadcaster()
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()

	b.Close()

	// Both channels should be closed
	for i, ch := range []chan string{ch1, ch2} {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("channel %d should be closed", i)
			}
		default:
			// Channel is closed, read the close
			_, ok := <-ch
			if ok {
				t.Errorf("channel %d should be closed", i)
			}
		}
	}
}

func TestLogBroadcasterSlowSubscriber(t *testing.T) {
	b := NewLogBroadcaster()

	// Create a subscriber but don't read from it
	_ = b.Subscribe()

	// Write more than the buffer size
	for i := 0; i < 150; i++ {
		_, _ = b.Write([]byte("line\n"))
	}

	// Should not block or panic
}

func TestLogBroadcasterConcurrentAccess(t *testing.T) {
	b := NewLogBroadcaster()

	var wg sync.WaitGroup

	// Concurrent subscribers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := b.Subscribe()
			time.Sleep(10 * time.Millisecond)
			b.Unsubscribe(ch)
		}()
	}

	// Concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = b.Write([]byte("concurrent write\n"))
			}
		}(i)
	}

	wg.Wait()
	b.Close()
}

func TestPipeReader(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	// Create a reader with multiple lines
	input := "line1\nline2\nline3\n"
	reader := strings.NewReader(input)

	// Run PipeReader in goroutine
	done := make(chan struct{})
	go func() {
		PipeReader(b, reader)
		close(done)
	}()

	// Collect received lines
	var received []string
	timeout := time.After(500 * time.Millisecond)

loop:
	for {
		select {
		case line, ok := <-ch:
			if !ok {
				break loop
			}
			received = append(received, line)
		case <-timeout:
			break loop
		}
	}

	<-done

	if len(received) != 3 {
		t.Errorf("received %d lines, want 3", len(received))
	}

	expected := []string{"line1\n", "line2\n", "line3\n"}
	for i, want := range expected {
		if i < len(received) && received[i] != want {
			t.Errorf("line %d: got %q, want %q", i, received[i], want)
		}
	}
}

func TestPipeReaderDrainsLineLargerThanScannerLimit(t *testing.T) {
	b := NewLogBroadcaster()
	line := strings.Repeat("x", 256<<10) + "\n"
	PipeReader(b, strings.NewReader(line))
	if got := strings.Join(b.Tail(), ""); got != line {
		t.Fatalf("lange regel niet volledig gedraind: got %d bytes, want %d", len(got), len(line))
	}
}

func TestPipeReaderClosesOnEOF(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	reader := bytes.NewReader([]byte("single line\n"))

	done := make(chan struct{})
	go func() {
		PipeReader(b, reader)
		close(done)
	}()

	// Wait for completion
	<-done

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			// Drain remaining message
			_, ok = <-ch
			if ok {
				t.Error("channel should eventually close")
			}
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for channel to close")
	}
}

func TestLogBroadcasterImplementsWriter(t *testing.T) {
	// Ensure LogBroadcaster implements io.Writer
	var _ interface{ Write([]byte) (int, error) } = &LogBroadcaster{}
}

// ============== EDGE CASE TESTS ==============

func TestLogBroadcasterWriteAfterClose(t *testing.T) {
	b := NewLogBroadcaster()
	_ = b.Subscribe()

	b.Close()

	// Write after close should not panic
	n, err := b.Write([]byte("after close\n"))
	if err != nil {
		t.Errorf("Write after close returned error: %v", err)
	}
	if n != 12 {
		t.Errorf("Write returned %d, want 12", n)
	}
}

func TestLogBroadcasterDoubleClose(t *testing.T) {
	b := NewLogBroadcaster()
	_ = b.Subscribe()

	b.Close()
	// Second close should not panic
	b.Close()
}

func TestLogBroadcasterUnsubscribeTwice(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	b.Unsubscribe(ch)
	// Second unsubscribe should not panic (channel already removed)
	b.Unsubscribe(ch)
}

func TestLogBroadcasterUnsubscribeNonExistent(t *testing.T) {
	b := NewLogBroadcaster()
	_ = b.Subscribe()

	// Unsubscribe a channel that was never subscribed
	otherCh := make(chan string)
	b.Unsubscribe(otherCh)
	// Should not panic
}

func TestLogBroadcasterEmptyWrite(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	n, err := b.Write([]byte{})
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Write returned %d, want 0", n)
	}

	// Empty string should still be sent
	select {
	case line := <-ch:
		if line != "" {
			t.Errorf("got %q, want empty string", line)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for empty line")
	}
}

func TestLogBroadcasterNoSubscribers(t *testing.T) {
	b := NewLogBroadcaster()

	// Write with no subscribers should not panic
	n, err := b.Write([]byte("no one listening\n"))
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != 17 {
		t.Errorf("Write returned %d, want 17", n)
	}
}

func TestLogBroadcasterSubscribeAfterClose(t *testing.T) {
	b := NewLogBroadcaster()
	b.Close()

	// Subscribe after close - channel should be added but immediately drained
	ch := b.Subscribe()

	// Write should work but channel won't receive (it's in closed state)
	_, _ = b.Write([]byte("test\n"))

	// Channel should be readable (might be empty or have messages)
	select {
	case <-ch:
		// Got a message or channel closed
	case <-time.After(50 * time.Millisecond):
		// Timeout is also acceptable (no subscribers when closed)
	}
}

func TestLogBroadcasterLargeMessage(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	// Write a large message (1MB)
	largeMsg := make([]byte, 1024*1024)
	for i := range largeMsg {
		largeMsg[i] = 'x'
	}

	n, err := b.Write(largeMsg)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(largeMsg) {
		t.Errorf("Write returned %d, want %d", n, len(largeMsg))
	}

	select {
	case line := <-ch:
		if len(line) != len(largeMsg) {
			t.Errorf("received %d bytes, want %d", len(line), len(largeMsg))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for large message")
	}
}

func TestLogBroadcasterRapidSubscribeUnsubscribe(t *testing.T) {
	b := NewLogBroadcaster()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := b.Subscribe()
			b.Unsubscribe(ch)
		}()
	}

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = b.Write([]byte("test\n"))
		}()
	}

	wg.Wait()
	b.Close()
}

func TestPipeReaderEmptyInput(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	reader := strings.NewReader("")

	done := make(chan struct{})
	go func() {
		PipeReader(b, reader)
		close(done)
	}()

	<-done

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed for empty input")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for channel to close")
	}
}

// ============== LOGSTORE (RETENTIE) ==============

// backdateRetired zet de klok van een gepensioneerde task terug voorbij de
// retentietermijn, zodat een test niet vijf minuten hoeft te wachten.
func backdateRetired(s *logStore, taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.retired[taskID]
	p.at = time.Now().Add(-s.policy.Keep - time.Second)
	s.retired[taskID] = p
}

// De kern: na retire zijn de logs nog opvraagbaar, met dezelfde inhoud, en een
// nieuwe lezer krijgt de geschiedenis plus een dicht kanaal (de task is voorbij,
// dus zijn logstroom ook — anders leest het als een vastgelopen node).
func TestLogStoreBewaartLogsNaHetAflopen(t *testing.T) {
	s := newLogStore()
	out, errB := NewLogBroadcaster(), NewLogBroadcaster()
	s.put("t1", out, errB)
	_, _ = out.Write([]byte("laatste woorden\n"))
	_, _ = errB.Write([]byte("panic: nee\n"))

	s.retire("t1")

	got := s.stdout("t1")
	if got == nil {
		t.Fatal("stdout is weg direct na retire — de retentie doet niets")
	}
	if tail := got.Tail(); len(tail) != 1 || tail[0] != "laatste woorden\n" {
		t.Fatalf("tail = %q, want [\"laatste woorden\\n\"]", tail)
	}
	if e := s.stderr("t1"); e == nil || len(e.Tail()) != 1 {
		t.Fatal("stderr is weg — juist daar staat waarom een proces viel")
	}

	ch := got.Subscribe()
	n := 0
	for range ch {
		n++
	}
	if n != 1 {
		t.Fatalf("Subscribe gaf %d regels, want 1 — en het kanaal moet sluiten", n)
	}
}

// Voorbij de termijn is de geschiedenis wél weg: een node die dagen
// restart-lussen draait mag geen logs opstapelen.
func TestLogStoreVerlooptNaDeTermijn(t *testing.T) {
	s := newLogStore()
	s.put("t1", NewLogBroadcaster(), NewLogBroadcaster())
	s.retire("t1")
	backdateRetired(s, "t1")

	if s.stdout("t1") != nil || s.stderr("t1") != nil {
		t.Fatal("logs zijn na de retentietermijn nog opvraagbaar")
	}

	// En de volgende retire ruimt de verlopen entry ook echt op (geen groei).
	s.put("t2", NewLogBroadcaster(), NewLogBroadcaster())
	s.retire("t2")
	s.mu.RLock()
	_, stale := s.retired["t1"]
	s.mu.RUnlock()
	if stale {
		t.Fatal("verlopen entry is niet opgeruimd")
	}
}

// Een task die nooit logs registreerde (mislukte start) mag retire niet laten
// klappen, en dubbel retire moet ook goed gaan — beide gebeuren in de runners.
func TestLogStoreRetireOnbekendEnDubbel(t *testing.T) {
	s := newLogStore()
	s.retire("bestaat-niet")

	s.put("t1", NewLogBroadcaster(), NewLogBroadcaster())
	s.retire("t1")
	s.retire("t1")
	if s.stdout("t1") == nil {
		t.Fatal("dubbel retire gooide de bewaarde logs weg")
	}
}

// Een hergebruikte taskID laat zijn pensioen achter zich: wie nu kijkt, kijkt
// naar de LOPENDE task, niet naar de vorige met dezelfde naam.
func TestLogStoreHergebruikteTaskIDKrijgtVerseLogs(t *testing.T) {
	s := newLogStore()
	oud := NewLogBroadcaster()
	s.put("t1", oud, NewLogBroadcaster())
	_, _ = oud.Write([]byte("oud\n"))
	s.retire("t1")

	nieuw := NewLogBroadcaster()
	s.put("t1", nieuw, NewLogBroadcaster())
	if got := s.stdout("t1"); got != nieuw {
		t.Fatal("stdout geeft de gepensioneerde broadcaster terug i.p.v. de lopende")
	}
	if tail := s.stdout("t1").Tail(); len(tail) != 0 {
		t.Fatalf("nieuwe task begint met %q, want een lege tail", tail)
	}
}

func TestPipeReaderNoNewlineAtEnd(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	// Input without trailing newline
	reader := strings.NewReader("line1\nline2")

	done := make(chan struct{})
	go func() {
		PipeReader(b, reader)
		close(done)
	}()

	var received []string
	timeout := time.After(500 * time.Millisecond)

loop:
	for {
		select {
		case line, ok := <-ch:
			if !ok {
				break loop
			}
			received = append(received, line)
		case <-timeout:
			break loop
		}
	}

	<-done

	// Should receive 2 lines (scanner handles no trailing newline)
	if len(received) != 2 {
		t.Errorf("received %d lines, want 2", len(received))
	}
}

// ============== LOGPOLICY ==============

// De tail-grootte en de bewaartijd komen uit de policy; nulvelden vallen terug
// op de defaults (50 regels, 5 minuten) zodat een kale config niets verandert.
func TestLogPolicyTailAndKeep(t *testing.T) {
	if got := (LogPolicy{}).orDefault(); got != DefaultLogPolicy {
		t.Fatalf("zero policy = %+v, want %+v", got, DefaultLogPolicy)
	}

	s := newLogStoreWith(LogPolicy{TailLines: 3, Keep: 20 * time.Millisecond})
	out, errB := s.newPair()
	s.put("t1", out, errB)
	for _, l := range []string{"1", "2", "3", "4", "5"} {
		_, _ = out.Write([]byte(l))
	}
	if tail := out.Tail(); len(tail) != 3 || tail[0] != "3" || tail[2] != "5" {
		t.Fatalf("tail with TailLines=3 = %v, want [3 4 5]", tail)
	}

	s.retire("t1")
	if s.stdout("t1") == nil {
		t.Fatal("logs gone right after retire; want them kept for Keep")
	}
	time.Sleep(40 * time.Millisecond)
	if s.stdout("t1") != nil {
		t.Fatal("logs still served after Keep elapsed")
	}
}
