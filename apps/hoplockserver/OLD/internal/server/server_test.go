package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xinix00/hoplockserver/internal/store"
)

func TestEndToEndCAS(t *testing.T) {
	srv := newTestServer(t, "")
	c := srv.Client()

	getMissing := mustDo(t, c, http.MethodGet, srv.URL+"/lease/a", "", "", "")
	if getMissing.StatusCode != http.StatusNotFound {
		t.Fatalf("GET missing: %d", getMissing.StatusCode)
	}

	create := mustDo(t, c, http.MethodPut, srv.URL+"/lease/a", `{"owner":"x"}`, "*", "")
	if create.StatusCode != http.StatusOK {
		t.Fatalf("create: %d", create.StatusCode)
	}
	etag1 := create.Header.Get("ETag")
	if etag1 == "" {
		t.Fatal("missing ETag")
	}

	conflict := mustDo(t, c, http.MethodPut, srv.URL+"/lease/a", `x`, "*", "")
	if conflict.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on second create, got %d", conflict.StatusCode)
	}

	get := mustDo(t, c, http.MethodGet, srv.URL+"/lease/a", "", "", "")
	if get.Header.Get("ETag") != etag1 {
		t.Fatalf("ETag mismatch on read")
	}

	bad := mustDo(t, c, http.MethodPut, srv.URL+"/lease/a", `{"owner":"y"}`, "", `"deadbeef"`)
	if bad.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on stale if-match, got %d", bad.StatusCode)
	}

	ok := mustDo(t, c, http.MethodPut, srv.URL+"/lease/a", `{"owner":"y"}`, "", etag1)
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on fresh if-match, got %d", ok.StatusCode)
	}
	etag2 := ok.Header.Get("ETag")
	if etag2 == etag1 {
		t.Fatal("expected new ETag after update")
	}

	delStale := mustDo(t, c, http.MethodDelete, srv.URL+"/lease/a", "", "", etag1)
	if delStale.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on stale delete, got %d", delStale.StatusCode)
	}
	delOK := mustDo(t, c, http.MethodDelete, srv.URL+"/lease/a", "", "", etag2)
	if delOK.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 on delete, got %d", delOK.StatusCode)
	}
}

func TestAPIKeyEnforced(t *testing.T) {
	srv := newTestServer(t, "secret")
	c := srv.Client()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/anything", nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without key, got %d", resp.StatusCode)
	}

	req.Header.Set("X-API-Key", "wrong")
	resp, err = c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong key, got %d", resp.StatusCode)
	}

	req.Header.Set("X-API-Key", "secret")
	resp, err = c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 with correct key on missing key, got %d", resp.StatusCode)
	}

	health, err := c.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("expected health 200, got %d", health.StatusCode)
	}
}

func newTestServer(t *testing.T, apiKey string) *httptest.Server {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(st, apiKey))
	t.Cleanup(srv.Close)
	return srv
}

func mustDo(t *testing.T, c *http.Client, method, url, body, ifNoneMatch, ifMatch string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}
