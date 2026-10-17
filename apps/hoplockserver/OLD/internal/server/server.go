// Package server exposes a hoplockserver-shaped HTTP API over a store.Store.
//
// The API surface is a deliberate minimum subset of S3's conditional-write
// protocol: GET/PUT/DELETE with If-Match and If-None-Match. It is enough to
// back any hoplock.Backend that talks to it via the client package.
//
// Authentication is a single shared API key in the X-API-Key header. If
// no key is configured, requests are allowed unauthenticated — useful for
// dev/standalone, dangerous for anything else.
package server

import (
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/xinix00/hoplockserver/internal/store"
)

// Server is an http.Handler exposing a store.Store over HTTP.
type Server struct {
	store  *store.Store
	apiKey string
	mux    *http.ServeMux
}

// New builds a Server backed by s. If apiKey is non-empty, every request
// outside /health must carry a matching X-API-Key header.
func New(s *store.Store, apiKey string) *Server {
	srv := &Server{store: s, apiKey: apiKey, mux: http.NewServeMux()}
	srv.mux.HandleFunc("/health", srv.handleHealth)
	srv.mux.HandleFunc("/", srv.handleKey)
	return srv
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	if !s.authorised(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.serveGet(w, r, key)
	case http.MethodPut:
		s.servePut(w, r, key)
	case http.MethodDelete:
		s.serveDelete(w, r, key)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) serveGet(w http.ResponseWriter, _ *http.Request, key string) {
	obj, err := s.store.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(obj.Body)
}

func (s *Server) servePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	cond := store.Condition{
		IfNoneMatch: r.Header.Get("If-None-Match"),
		IfMatch:     r.Header.Get("If-Match"),
	}
	if cond.IfNoneMatch != "" && cond.IfNoneMatch != "*" {
		http.Error(w, "only If-None-Match: * is supported", http.StatusBadRequest)
		return
	}
	obj, err := s.store.Put(key, body, cond)
	switch {
	case errors.Is(err, store.ErrPrecondition):
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	case errors.Is(err, store.ErrBadKey):
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", obj.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) serveDelete(w http.ResponseWriter, r *http.Request, key string) {
	err := s.store.Delete(key, store.Condition{IfMatch: r.Header.Get("If-Match")})
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, store.ErrPrecondition):
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	case errors.Is(err, store.ErrBadKey):
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) authorised(r *http.Request) bool {
	if s.apiKey == "" {
		return true
	}
	got := r.Header.Get("X-API-Key")
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.apiKey)) == 1
}
