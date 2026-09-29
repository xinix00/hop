package runner

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xinix00/hop/internal/types"
)

// --- downloadArtifact edge cases ---

func TestDownloadArtifactNil(t *testing.T) {
	err := downloadArtifact(nil, "/tmp/test")
	if err != nil {
		t.Errorf("downloadArtifact(nil) = %v, want nil", err)
	}
}

func TestDownloadArtifactEmptyURL(t *testing.T) {
	err := downloadArtifact(&types.Artifact{URL: ""}, "/tmp/test")
	if err != nil {
		t.Errorf("downloadArtifact(empty URL) = %v, want nil", err)
	}
}

func TestDownloadArtifactUnsupportedScheme(t *testing.T) {
	err := downloadArtifact(&types.Artifact{URL: "ftp://example.com/file.tar.gz"}, "/tmp/test")
	if err == nil {
		t.Error("downloadArtifact(ftp://) should return error")
	}
	if !strings.Contains(err.Error(), "unsupported URL scheme") {
		t.Errorf("Error = %q, want 'unsupported URL scheme'", err.Error())
	}
}

// --- HTTP download tests ---

func TestDownloadHTTPSuccess(t *testing.T) {
	content := []byte("binary content here")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "testfile")

	artifact := &types.Artifact{URL: srv.URL + "/app.bin"}
	err := downloadHTTP(artifact, destPath)
	if err != nil {
		t.Fatalf("downloadHTTP failed: %v", err)
	}

	data, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("Downloaded content = %q, want %q", data, content)
	}
}

func TestDownloadHTTPBasicAuth(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "testfile")

	artifact := &types.Artifact{
		URL: srv.URL + "/app.bin",
		Auth: map[string]string{
			"username": "deploy",
			"password": "secret123",
		},
	}
	err := downloadHTTP(artifact, destPath)
	if err != nil {
		t.Fatalf("downloadHTTP failed: %v", err)
	}

	if !strings.HasPrefix(receivedAuth, "Basic ") {
		t.Errorf("Authorization = %q, want Basic auth header", receivedAuth)
	}
}

func TestDownloadHTTPCustomHeaders(t *testing.T) {
	var receivedToken string
	var receivedAPIKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedToken = r.Header.Get("Authorization")
		receivedAPIKey = r.Header.Get("X-API-Key")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "testfile")

	artifact := &types.Artifact{
		URL: srv.URL + "/app.bin",
		Headers: map[string]string{
			"Authorization": "Bearer token123",
			"X-API-Key":     "secret-key",
		},
	}
	err := downloadHTTP(artifact, destPath)
	if err != nil {
		t.Fatalf("downloadHTTP failed: %v", err)
	}

	if receivedToken != "Bearer token123" {
		t.Errorf("Authorization = %q, want %q", receivedToken, "Bearer token123")
	}
	if receivedAPIKey != "secret-key" {
		t.Errorf("X-API-Key = %q, want %q", receivedAPIKey, "secret-key")
	}
}

func TestDownloadHTTP404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "testfile")

	artifact := &types.Artifact{URL: srv.URL + "/missing.bin"}
	err := downloadHTTP(artifact, destPath)
	if err == nil {
		t.Error("downloadHTTP should return error for 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("Error = %q, want to contain '404'", err.Error())
	}
}

// --- Tar extraction tests ---

func TestExtractTarGzValid(t *testing.T) {
	destDir := t.TempDir()
	tarGzPath := filepath.Join(destDir, "test.tar.gz")

	// Create a valid .tar.gz
	createTarGz(t, tarGzPath, map[string]string{
		"hello.txt":      "Hello, World!",
		"dir/nested.txt": "Nested content",
	})

	extractDir := t.TempDir()
	err := extractTarGz(tarGzPath, extractDir)
	if err != nil {
		t.Fatalf("extractTarGz failed: %v", err)
	}

	// Verify files
	content, err := os.ReadFile(filepath.Join(extractDir, "hello.txt"))
	if err != nil {
		t.Fatalf("ReadFile hello.txt: %v", err)
	}
	if string(content) != "Hello, World!" {
		t.Errorf("hello.txt = %q, want %q", content, "Hello, World!")
	}

	content, err = os.ReadFile(filepath.Join(extractDir, "dir", "nested.txt"))
	if err != nil {
		t.Fatalf("ReadFile dir/nested.txt: %v", err)
	}
	if string(content) != "Nested content" {
		t.Errorf("dir/nested.txt = %q, want %q", content, "Nested content")
	}
}

func TestExtractTarPathTraversal(t *testing.T) {
	destDir := t.TempDir()

	// Create tar with path traversal attempt
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	_ = tw.WriteHeader(&tar.Header{
		Name: "../../../etc/evil",
		Mode: 0644,
		Size: 4,
	})
	_, _ = tw.Write([]byte("evil"))
	tw.Close()

	err := extractTar(&buf, destDir)
	if err == nil {
		t.Error("extractTar should reject path traversal")
	}
	if !strings.Contains(err.Error(), "illegal file path") {
		t.Errorf("Error = %q, want 'illegal file path'", err.Error())
	}
}

// --- Zip extraction tests ---

func TestExtractZipValid(t *testing.T) {
	destDir := t.TempDir()
	zipPath := filepath.Join(destDir, "test.zip")

	// Create a valid .zip
	createZip(t, zipPath, map[string]string{
		"hello.txt":      "Hello, World!",
		"dir/nested.txt": "Nested content",
	})

	extractDir := t.TempDir()
	err := extractZip(zipPath, extractDir)
	if err != nil {
		t.Fatalf("extractZip failed: %v", err)
	}

	// Verify files
	content, err := os.ReadFile(filepath.Join(extractDir, "hello.txt"))
	if err != nil {
		t.Fatalf("ReadFile hello.txt: %v", err)
	}
	if string(content) != "Hello, World!" {
		t.Errorf("hello.txt = %q, want %q", content, "Hello, World!")
	}

	content, err = os.ReadFile(filepath.Join(extractDir, "dir", "nested.txt"))
	if err != nil {
		t.Fatalf("ReadFile dir/nested.txt: %v", err)
	}
	if string(content) != "Nested content" {
		t.Errorf("dir/nested.txt = %q, want %q", content, "Nested content")
	}
}

func TestExtractZipPathTraversal(t *testing.T) {
	destDir := t.TempDir()
	zipPath := filepath.Join(destDir, "evil.zip")

	// Create zip with path traversal
	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	w, err := zw.Create("../../../etc/evil")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("evil"))
	zw.Close()
	f.Close()

	extractDir := t.TempDir()
	err = extractZip(zipPath, extractDir)
	if err == nil {
		t.Error("extractZip should reject path traversal")
	}
	if !strings.Contains(err.Error(), "illegal file path") {
		t.Errorf("Error = %q, want 'illegal file path'", err.Error())
	}
}

// --- Full download + extract integration ---

func TestDownloadHTTPWithExtractTarGz(t *testing.T) {
	// Create a tar.gz to serve
	tarBuf := createTarGzBytes(t, map[string]string{
		"app":        "binary content",
		"config.yml": "port: 8080",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(tarBuf)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	artifact := &types.Artifact{
		URL:     srv.URL + "/app.tar.gz",
		Extract: "tar.gz",
	}
	err := downloadArtifact(artifact, destDir)
	if err != nil {
		t.Fatalf("downloadArtifact failed: %v", err)
	}

	// Verify extracted files
	content, err := os.ReadFile(filepath.Join(destDir, "app"))
	if err != nil {
		t.Fatalf("ReadFile app: %v", err)
	}
	if string(content) != "binary content" {
		t.Errorf("app = %q, want %q", content, "binary content")
	}
}

func TestDownloadHTTPWithExtractZip(t *testing.T) {
	// Create a zip to serve
	zipBuf := createZipBytes(t, map[string]string{
		"app":        "binary content",
		"config.yml": "port: 8080",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(zipBuf)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	artifact := &types.Artifact{
		URL:     srv.URL + "/app.zip",
		Extract: "zip",
	}
	err := downloadArtifact(artifact, destDir)
	if err != nil {
		t.Fatalf("downloadArtifact failed: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(destDir, "app"))
	if err != nil {
		t.Fatalf("ReadFile app: %v", err)
	}
	if string(content) != "binary content" {
		t.Errorf("app = %q, want %q", content, "binary content")
	}
}

func TestDownloadHTTPRawBinary(t *testing.T) {
	content := []byte("#!/bin/sh\necho hello")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	destDir := t.TempDir()
	artifact := &types.Artifact{
		URL: srv.URL + "/myapp",
		// No Extract = raw binary download
	}
	err := downloadArtifact(artifact, destDir)
	if err != nil {
		t.Fatalf("downloadArtifact failed: %v", err)
	}

	destPath := filepath.Join(destDir, "myapp")
	data, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("Downloaded content mismatch")
	}

	// Verify executable permission
	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&0111 == 0 {
		t.Error("Downloaded binary should be executable")
	}
}

func TestDownloadArtifactUnsupportedExtract(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	}))
	defer srv.Close()

	destDir := t.TempDir()
	artifact := &types.Artifact{
		URL:     srv.URL + "/app.rar",
		Extract: "rar",
	}
	err := downloadArtifact(artifact, destDir)
	if err == nil {
		t.Error("Should reject unsupported extract type")
	}
	if !strings.Contains(err.Error(), "unsupported extract type") {
		t.Errorf("Error = %q, want 'unsupported extract type'", err.Error())
	}
}

// --- S3 tests ---

func TestDownloadS3MissingCredentials(t *testing.T) {
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "testfile")

	artifact := &types.Artifact{
		URL:  "s3://mybucket/mykey",
		Auth: map[string]string{}, // No credentials
	}
	err := downloadS3(artifact, destPath)
	if err == nil {
		t.Error("downloadS3 should require credentials")
	}
	if !strings.Contains(err.Error(), "access_key") {
		t.Errorf("Error = %q, want mention of 'access_key'", err.Error())
	}
}

func TestSignS3GetRequest(t *testing.T) {
	url, headers := signS3GetRequest("mybucket.s3.us-east-1.amazonaws.com", "path/to/file.tar.gz", "us-east-1", "AKIAEXAMPLE", "secretkey")

	if !strings.Contains(url, "mybucket.s3.us-east-1.amazonaws.com") {
		t.Errorf("URL = %q, want to contain host", url)
	}
	if !strings.Contains(url, "path/to/file.tar.gz") {
		t.Errorf("URL = %q, want to contain key path", url)
	}
	if !strings.HasPrefix(url, "https://") {
		t.Errorf("URL = %q, want https scheme", url)
	}

	auth := headers["Authorization"]
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
		t.Errorf("Authorization = %q, want AWS4-HMAC-SHA256 prefix", auth)
	}
	if !strings.Contains(auth, "AKIAEXAMPLE") {
		t.Errorf("Authorization = %q, want to contain access key", auth)
	}
	if !strings.Contains(auth, "us-east-1/s3/aws4_request") {
		t.Errorf("Authorization = %q, want credential scope", auth)
	}

	if headers["x-amz-date"] == "" {
		t.Error("Missing x-amz-date header")
	}
}

func TestSignS3GetRequestCustomEndpoint(t *testing.T) {
	url, headers := signS3GetRequest("haas-builds.fsn1.your-objectstorage.com", "ravendb/file.tar.bz2", "us-east-1", "AKIAEXAMPLE", "secretkey")

	if !strings.Contains(url, "haas-builds.fsn1.your-objectstorage.com/ravendb/file.tar.bz2") {
		t.Errorf("URL = %q, want custom endpoint + key", url)
	}
	if headers["Authorization"] == "" {
		t.Error("Missing Authorization header")
	}
}

func TestSignS3GetRequestDefaultRegion(t *testing.T) {
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "testfile")

	artifact := &types.Artifact{
		URL: "s3://mybucket/mykey",
		Auth: map[string]string{
			"access_key": "AKIAEXAMPLE",
			"secret_key": "secretkey",
			// No region — should default to us-east-1
		},
	}

	// This will fail at the HTTP level (no real S3), but we can verify it doesn't
	// fail at the credential/region parsing level
	err := downloadS3(artifact, destPath)
	if err == nil {
		t.Skip("Unexpected success (real S3 bucket?)")
	}
	// Should fail with HTTP error, not credential error
	if strings.Contains(err.Error(), "access_key") {
		t.Errorf("Should not fail on credentials: %v", err)
	}
}

// --- Test helpers ---

func createTarGz(t *testing.T, path string, files map[string]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	tw := tar.NewWriter(gw)

	for name, content := range files {
		_ = tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0644,
			Size: int64(len(content)),
		})
		_, _ = tw.Write([]byte(content))
	}

	tw.Close()
	gw.Close()
}

func createTarGzBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer

	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	for name, content := range files {
		_ = tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0644,
			Size: int64(len(content)),
		})
		_, _ = tw.Write([]byte(content))
	}

	tw.Close()
	gw.Close()
	return buf.Bytes()
}

func createZip(t *testing.T, path string, files map[string]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(content))
	}
	zw.Close()
}

func createZipBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.WriteString(w, content)
	}
	zw.Close()
	return buf.Bytes()
}
