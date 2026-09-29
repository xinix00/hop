package leader

import "sync"

// EventBus broadcasts notifications to SSE subscribers.
// Channels have buffer 1 for natural coalescing of identical events.
// Latest event always wins: if the channel is full, the stale event
// is drained and replaced so subscribers always see the most recent state.
type EventBus struct {
	mu        sync.Mutex
	listeners []chan string
}

// NewEventBus creates a new event bus
func NewEventBus() *EventBus {
	return &EventBus{}
}

// Subscribe returns a channel that receives job names on state changes
func (e *EventBus) Subscribe() chan string {
	ch := make(chan string, 1)
	e.mu.Lock()
	e.listeners = append(e.listeners, ch)
	e.mu.Unlock()
	return ch
}

// Unsubscribe removes a listener
func (e *EventBus) Unsubscribe(ch chan string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, l := range e.listeners {
		if l == ch {
			e.listeners = append(e.listeners[:i], e.listeners[i+1:]...)
			close(ch)
			return
		}
	}
}

// Notify sends a notification to all subscribers.
// If the channel is full, the stale event is replaced with the latest.
func (e *EventBus) Notify(topic string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ch := range e.listeners {
		select {
		case ch <- topic:
		default:
			// Drain stale event, send latest
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- topic:
			default:
			}
		}
	}
}
