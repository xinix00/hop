package store

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	s := newStore(t)

	if _, err := s.Get("lease/foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	got, err := s.Put("lease/foo", []byte(`{"owner":"a"}`), Condition{IfNoneMatch: "*"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got.ETag == "" {
		t.Fatal("expected ETag")
	}

	if _, err := s.Put("lease/foo", []byte("x"), Condition{IfNoneMatch: "*"}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("expected ErrPrecondition for if-none-match on existing key, got %v", err)
	}

	got2, err := s.Get("lease/foo")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got2.Body) != `{"owner":"a"}` || got2.ETag != got.ETag {
		t.Fatalf("get mismatch: %+v", got2)
	}

	if _, err := s.Put("lease/foo", []byte("y"), Condition{IfMatch: `"deadbeef"`}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("expected ErrPrecondition for stale if-match, got %v", err)
	}

	got3, err := s.Put("lease/foo", []byte(`{"owner":"b"}`), Condition{IfMatch: got.ETag})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got3.ETag == got.ETag {
		t.Fatalf("expected new ETag, got same %q", got3.ETag)
	}

	if err := s.Delete("lease/foo", Condition{IfMatch: got.ETag}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("expected ErrPrecondition for stale delete, got %v", err)
	}
	if err := s.Delete("lease/foo", Condition{IfMatch: got3.ETag}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.Delete("lease/foo", Condition{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound on second delete, got %v", err)
	}
}

func TestEtagIsDeterministic(t *testing.T) {
	s := newStore(t)
	a, err := s.Put("k", []byte("hello"), Condition{IfNoneMatch: "*"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("k", Condition{}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Put("k", []byte("hello"), Condition{IfNoneMatch: "*"})
	if err != nil {
		t.Fatal(err)
	}
	if a.ETag != b.ETag {
		t.Fatalf("ETag should be deterministic on body, got %q vs %q", a.ETag, b.ETag)
	}
}

func TestPathTraversalRejected(t *testing.T) {
	s := newStore(t)
	for _, key := range []string{"../escape", "/abs", "foo/../../escape", ""} {
		if _, err := s.Put(key, []byte("x"), Condition{IfNoneMatch: "*"}); !errors.Is(err, ErrBadKey) {
			t.Errorf("key %q: expected ErrBadKey, got %v", key, err)
		}
	}
}

func TestNestedKeyCreatesDirs(t *testing.T) {
	s := newStore(t)
	if _, err := s.Put("a/b/c/lease.json", []byte("x"), Condition{IfNoneMatch: "*"}); err != nil {
		t.Fatalf("nested put: %v", err)
	}
	got, err := s.Get("a/b/c/lease.json")
	if err != nil || string(got.Body) != "x" {
		t.Fatalf("nested get: body=%q err=%v", got.Body, err)
	}
}

func newStore(t *testing.T) *Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
