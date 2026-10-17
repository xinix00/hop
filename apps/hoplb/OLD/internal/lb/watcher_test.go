package lb

import (
	"testing"

	"github.com/xinix00/hoplib"
)

func TestClassifyEvent(t *testing.T) {
	relevant := map[string]struct{}{"api": {}}
	known := map[string]*hoplib.Job{"api": {}, "batch": {}}

	cases := []struct {
		name, line, wantJob string
		wantFull            bool
	}{
		{"job event on relevant job", `data: {"name":"api"}`, "api", true},
		{"job event on known irrelevant job (tags may have changed)", `data: {"name":"batch"}`, "batch", true},
		{"job event on unknown job", `data: {"name":"new"}`, "new", true},
		{"task event on relevant job", `data: {"job":"api","event":"started"}`, "api", false},
		{"task event on known irrelevant job", `data: {"job":"batch","event":"crash"}`, "", false},
		{"task event on unknown job", `data: {"job":"new","event":"started"}`, "new", true},
		{"ping", `data: {}`, "", false},
		{"garbage", `data: not json`, "", false},
	}
	for _, c := range cases {
		job, full := classifyEvent(c.line, relevant, known)
		if job != c.wantJob || full != c.wantFull {
			t.Errorf("%s: classifyEvent(%q) = (%q, %v), want (%q, %v)", c.name, c.line, job, full, c.wantJob, c.wantFull)
		}
	}
}
