package hoplib

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// AuthHeader carries the HMAC request signature. Must match hop's
// pkg/httputil.AuthHeader and signing scheme exactly.
const AuthHeader = "X-Hop-Auth"

// Client is an HTTP client with API key authentication for hop agents.
type Client struct {
	HTTP   *http.Client
	APIKey string
}

// NewClient creates a new authenticated client
func NewClient(apiKey string) *Client {
	return &Client{
		HTTP:   &http.Client{Timeout: 10 * time.Second},
		APIKey: apiKey,
	}
}

// Sign sets the X-Hop-Auth header: HMAC-SHA256(key, METHOD\nPATH\nsha256(body)).
// body is the exact bytes sent (nil for GETs). The key never travels on the
// wire — only this signature does. No-op when APIKey is empty (auth disabled).
func (c *Client) Sign(req *http.Request, body []byte) {
	if c.APIKey == "" {
		return
	}
	sum := sha256.Sum256(body)
	msg := req.Method + "\n" + req.URL.Path + "\n" + hex.EncodeToString(sum[:])
	mac := hmac.New(sha256.New, []byte(c.APIKey))
	mac.Write([]byte(msg))
	req.Header.Set(AuthHeader, hex.EncodeToString(mac.Sum(nil)))
}

// Get performs an authenticated GET request
func (c *Client) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	c.Sign(req, nil)
	return c.HTTP.Do(req)
}

// Fetch performs an authenticated GET and decodes the JSON response
func Fetch[T any](c *Client, url string) (T, error) {
	var result T
	resp, err := c.Get(url)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("status %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result, err
}

// ParseJobFromSSE extracts the job name from an SSE data line.
// Handles both job events ({"name":"..."}) and task events ({"job":"...","event":"..."}).
func ParseJobFromSSE(line string) string {
	data := strings.TrimPrefix(line, "data:")
	data = strings.TrimSpace(data)
	var ev struct {
		Name string `json:"name"`
		Job  string `json:"job"`
	}
	_ = json.Unmarshal([]byte(data), &ev)
	if ev.Name != "" {
		return ev.Name
	}
	return ev.Job
}

// API response types shared across satellite services

// Agent represents an hop agent
type Agent struct {
	ID       string    `json:"id"`
	Endpoint string    `json:"endpoint"`
	LastSeen time.Time `json:"last_seen,omitempty"`
}

// Task represents an hop task
type Task struct {
	ID           string         `json:"id"`
	JobName      string         `json:"job_name"`
	State        string         `json:"state"`
	Ports        map[string]int `json:"ports,omitempty"`
	Pid          int            `json:"pid,omitempty"`
	RestartCount int            `json:"restart_count,omitempty"`
	StartedAt    time.Time      `json:"started_at,omitempty"`
	CPUPercent   float64        `json:"cpu_percent,omitempty"`
	MemPercent   float64        `json:"mem_percent,omitempty"`
}

// Job represents an hop job
type Job struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Command     string            `json:"command,omitempty"`
	Count       int               `json:"count,omitempty"`
	CPUShares   int               `json:"cpu_shares,omitempty"`
	MemoryLimit uint64            `json:"memory_limit,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
}
