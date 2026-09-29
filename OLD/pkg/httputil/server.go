package httputil

// server.go — de stop-lifecycle rond leanhttp.Serve. leanhttp bedient tot de
// listener sluit; hop moet daarnaast kunnen STOPPEN met alles erop en eraan:
// een agent die afsluit en een ex-leader die :9080 loslaat mogen geen
// handler-goroutines of open verbindingen achterlaten. Die eigenaarschap is
// hop-beleid, geen transportmateriaal — daarom woont hij hier en niet in lean.
//
// Close is onmiddellijk: listener én elke geaccepteerde verbinding dicht,
// lopende handlers incluis. Bewust geen graceful drain — wie netjes wil
// eindigen stopt eerst met produceren (streams cancelen via de lifecycle-ctx,
// geen werk meer aannemen) en sluit dan; een drain-timer zou streams alsnog
// afkappen of altijd zijn volle budget verbranden aan idle keep-alives.

import (
	"errors"
	"net"
	"sync"

	"github.com/xinix00/lean/leanhttp"
)

// ErrServerClosed is returned by ListenAndServe after Close.
var ErrServerClosed = errors.New("httputil: server closed")

// Server serves one address and can be stopped. Create one with NewServer.
type Server struct {
	addr string
	h    leanhttp.Handler

	mu     sync.Mutex
	l      net.Listener
	conns  map[*trackedConn]struct{}
	closed bool
}

// NewServer prepares a server for addr. Nothing listens until ListenAndServe.
// leanhttp caps request heads and bodies and carries the slowloris/idle/write
// deadlines itself, so there is nothing to configure here.
func NewServer(addr string, h leanhttp.Handler) *Server {
	return &Server{addr: addr, h: h, conns: make(map[*trackedConn]struct{})}
}

// Addr is the address the server was built for.
func (s *Server) Addr() string { return s.addr }

// ListenAndServe serves until Close, then returns ErrServerClosed.
func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed { // Close won the race: do not start accepting after all
		s.mu.Unlock()
		l.Close()
		return ErrServerClosed
	}
	s.l = l
	s.mu.Unlock()

	// leanhttp owns a goroutine per accepted connection. Wrap the listener so
	// the Server retains ownership of those connections too; closing only the
	// listening socket would leave existing handlers and their goroutines
	// untouched.
	err = leanhttp.Serve(&trackingListener{Listener: l, server: s}, s.h)
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrServerClosed // the accept error was our own Close
	}
	return err
}

// Close stops accepting and closes every accepted connection. See the package
// comment for why there is no graceful variant.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	l := s.l
	conns := make([]*trackedConn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	var closeErr error
	if l != nil {
		if err := l.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = errors.Join(closeErr, err)
		}
	}
	for _, conn := range conns {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = errors.Join(closeErr, err)
		}
	}
	return closeErr
}

// trackingListener publishes every accepted connection before leanhttp starts
// its serving goroutine. Close and Accept serialize on Server.mu, so a
// connection accepted across the close boundary is either in the snapshot or
// rejected and closed here; there is no unowned gap.
type trackingListener struct {
	net.Listener
	server *Server
}

func (l *trackingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tracked := &trackedConn{Conn: conn, server: l.server}
	l.server.mu.Lock()
	if l.server.closed {
		l.server.mu.Unlock()
		_ = conn.Close()
		return nil, ErrServerClosed
	}
	l.server.conns[tracked] = struct{}{}
	l.server.mu.Unlock()
	return tracked, nil
}

type trackedConn struct {
	net.Conn
	server *Server
	once   sync.Once
	err    error
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.server.mu.Lock()
		delete(c.server.conns, c)
		c.server.mu.Unlock()
	})
	return c.err
}
