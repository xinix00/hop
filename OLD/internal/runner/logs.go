package runner

import (
	"bufio"
	"io"
	"sync"
	"time"
)

// defaultTailLines is the ring-buffer size of a broadcaster made with
// NewLogBroadcaster; runners size theirs from their LogPolicy.
const defaultTailLines = 50

// LogPolicy is what a runner keeps of a task's output: the last TailLines
// lines per stream while it runs, and — after it stopped — the same tail for
// Keep. Both come from the node config (runner.log_tail_lines,
// runner.log_keep_seconds); the defaults are deliberately small because hop
// also runs on boards with a few hundred MB.
type LogPolicy struct {
	TailLines int
	Keep      time.Duration
}

// DefaultLogPolicy: 50 lines, 5 minutes.
var DefaultLogPolicy = LogPolicy{TailLines: defaultTailLines, Keep: 5 * time.Minute}

// orDefault fills zero fields from DefaultLogPolicy.
func (p LogPolicy) orDefault() LogPolicy {
	if p.TailLines <= 0 {
		p.TailLines = DefaultLogPolicy.TailLines
	}
	if p.Keep <= 0 {
		p.Keep = DefaultLogPolicy.Keep
	}
	return p
}

// LogBroadcaster broadcasts log lines to multiple listeners
// and keeps the last N lines in a ring buffer for post-crash debugging.
type LogBroadcaster struct {
	listeners []chan string
	tail      []string
	tailPos   int
	tailCount int
	closed    bool
	mu        sync.RWMutex
}

// NewLogBroadcaster creates a broadcaster with the default tail size.
func NewLogBroadcaster() *LogBroadcaster { return newLogBroadcasterN(defaultTailLines) }

// newLogBroadcasterN creates a broadcaster keeping the last n lines.
func newLogBroadcasterN(n int) *LogBroadcaster {
	if n <= 0 {
		n = defaultTailLines
	}
	return &LogBroadcaster{
		listeners: make([]chan string, 0),
		tail:      make([]string, n),
	}
}

// Write implements io.Writer interface
func (b *LogBroadcaster) Write(p []byte) (n int, err error) {
	line := string(p)

	b.mu.Lock()
	b.tail[b.tailPos%len(b.tail)] = line
	b.tailPos++
	if b.tailCount < len(b.tail) {
		b.tailCount++
	}
	for _, ch := range b.listeners {
		select {
		case ch <- line:
		default:
		}
	}
	b.mu.Unlock()

	return len(p), nil
}

// Tail returns the last N lines (up to 50).
func (b *LogBroadcaster) Tail() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	lines := make([]string, b.tailCount)
	start := b.tailPos - b.tailCount
	for i := range b.tailCount {
		lines[i] = b.tail[(start+i)%len(b.tail)]
	}
	return lines
}

// Subscribe adds a new listener and returns a channel for log lines.
// The tail buffer is pushed first so the subscriber sees recent history.
//
// On a finished task the channel is closed right after that history: the task is
// over, so its log stream is over too. Without this, asking a dead task for its
// logs would hand over the history and then hang until the client gave up —
// which reads like a stalled node instead of a completed one.
func (b *LogBroadcaster) Subscribe() chan string {
	ch := make(chan string, 100)

	b.mu.Lock()
	// Push tail history
	start := b.tailPos - b.tailCount
	for i := range b.tailCount {
		ch <- b.tail[(start+i)%len(b.tail)]
	}
	if b.closed {
		close(ch)
	} else {
		b.listeners = append(b.listeners, ch)
	}
	b.mu.Unlock()

	return ch
}

// Unsubscribe removes a listener
func (b *LogBroadcaster) Unsubscribe(ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, listener := range b.listeners {
		if listener == ch {
			b.listeners = append(b.listeners[:i], b.listeners[i+1:]...)
			close(ch)
			break
		}
	}
}

// Close closes all listeners (call when process exits)
func (b *LogBroadcaster) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.listeners {
		close(ch)
	}
	b.listeners = nil
	// The tail stays readable — that is the whole point of keeping a finished
	// task's logs around. closed only means "no more lines will come".
	b.closed = true
}

// Hoe lang de logs van een AFGELOPEN task opvraagbaar blijven staat in
// LogPolicy.Keep (default 5 minuten): lang genoeg om ná de melding "task
// failed" te gaan kijken, kort genoeg dat een node die dagen restart-lussen
// draait geen geschiedenis opstapelt.

// logStore is de log-boekhouding van één runner: de broadcasters van de LOPENDE
// tasks, plus die van net-afgelopen tasks — die gaan niet weg maar met pensioen
// en blijven logRetention opvraagbaar.
//
// Alle drie de runners (exec, docker, hop) gebruiken deze ene store, want het
// probleem was voor alle drie hetzelfde: bij het opruimen van een task ging zijn
// broadcaster meteen mee, dus wie een gevallen task om zijn logs vroeg kreeg
// "task not found" — de log was weg op precies het moment dat hij telde, en op
// een headless node bestond het waarom dan nergens meer. In een restart-lus is
// dat elke keer.
type logStore struct {
	policy  LogPolicy
	mu      sync.RWMutex
	live    map[string]logPair
	retired map[string]logPair
}

// logPair zijn de twee logstromen van één task; at is het moment van pensioen
// (nul zolang de task loopt).
type logPair struct {
	stdout *LogBroadcaster
	stderr *LogBroadcaster
	at     time.Time
}

func newLogStore() *logStore { return newLogStoreWith(DefaultLogPolicy) }

// newLogStoreWith maakt een store met de gegeven policy (nulvelden = default).
func newLogStoreWith(p LogPolicy) *logStore {
	return &logStore{
		policy:  p.orDefault(),
		live:    make(map[string]logPair),
		retired: make(map[string]logPair),
	}
}

// newPair maakt de twee broadcasters van een task, met de tail-grootte van
// deze store. Registreren doet de aanroeper met put.
func (s *logStore) newPair() (stdout, stderr *LogBroadcaster) {
	return newLogBroadcasterN(s.policy.TailLines), newLogBroadcasterN(s.policy.TailLines)
}

// put legt de broadcasters van een startende task vast. Een hergebruikte taskID
// laat zijn pensioen achter zich: de nieuwe logs zijn dan de logs.
func (s *logStore) put(taskID string, stdout, stderr *LogBroadcaster) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.live[taskID] = logPair{stdout: stdout, stderr: stderr}
	delete(s.retired, taskID)
}

// retire stuurt de logs van een afgelopen task met pensioen: Close() sluit
// lopende tails netjes af (er komt geen regel meer bij) maar laat de tail
// leesbaar, nog logRetention lang. Idempotent, en een no-op voor een task die
// nooit logs registreerde (mislukte start).
func (s *logStore) retire(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if p, ok := s.live[taskID]; ok {
		if p.stdout != nil {
			p.stdout.Close()
		}
		if p.stderr != nil {
			p.stderr.Close()
		}
		p.at = time.Now()
		s.retired[taskID] = p
		delete(s.live, taskID)
	}

	// Opruimen gebeurt hier en niet op een achtergrond-timer: het juiste moment
	// om verlopen geschiedenis te lozen is precies wanneer er weer iets bij komt.
	for id, p := range s.retired {
		if time.Since(p.at) > s.policy.Keep {
			delete(s.retired, id)
		}
	}
}

// stdout geeft de broadcaster van een lopende task, of die van een task die
// minder dan logRetention geleden afliep (anders nil).
func (s *logStore) stdout(taskID string) *LogBroadcaster {
	p, _ := s.lookup(taskID)
	return p.stdout
}

// stderr doet hetzelfde voor de foutstroom.
func (s *logStore) stderr(taskID string) *LogBroadcaster {
	p, _ := s.lookup(taskID)
	return p.stderr
}

func (s *logStore) lookup(taskID string) (logPair, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p, ok := s.live[taskID]; ok {
		return p, true
	}
	if p, ok := s.retired[taskID]; ok && time.Since(p.at) <= s.policy.Keep {
		return p, true
	}
	return logPair{}, false
}

// PipeReader reads from reader and broadcasts to broadcaster until EOF
func PipeReader(broadcaster *LogBroadcaster, reader io.Reader) {
	buffered := bufio.NewReaderSize(reader, 64<<10)
	for {
		fragment, err := buffered.ReadSlice('\n')
		if len(fragment) > 0 {
			// ReadSlice geeft ErrBufferFull voor een lange regel. Schrijf het
			// fragment meteen weg en blijf lezen: zo blijft de OS-pipe altijd
			// draineren zonder de volledige regel in geheugen te hoeven houden.
			_, _ = broadcaster.Write(fragment)
		}
		if err == nil || err == bufio.ErrBufferFull {
			continue
		}
		break
	}
	// Reader closed (process exited), close broadcaster
	broadcaster.Close()
}
