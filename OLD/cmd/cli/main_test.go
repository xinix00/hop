package main

import "testing"

// TestContract_ApplyCount pins that `run apply --count` reaches Job.Count.
// The docs use --count everywhere; if the wiring is dropped this goes red.
func TestContract_ApplyCount(t *testing.T) {
	job := buildJob("web", "./app", "", "", 3, 0, "", -1,
		nil, nil, nil, nil, "", "", "", 0, "rolling")
	if job.Count != 3 {
		t.Fatalf("--count not wired to Job.Count: got %d, want 3", job.Count)
	}

	// count -1 (run on all agents) must survive too.
	daemon := buildJob("dns", "./dns", "", "", -1, 0, "", -1,
		nil, nil, nil, nil, "", "", "", 0, "rolling")
	if daemon.Count != -1 {
		t.Fatalf("--count -1 not preserved: got %d", daemon.Count)
	}
}

// De herhaalbare flags gaan via flag.Value (stringList) en env-paren splitsen
// alleen op de EERSTE '=': een waarde mag komma's en '='-tekens bevatten. De
// oude comma-join brak "FOO=a,b" stil in twee halve paren.
func TestParsePairsHoudtKommasEnIsgelijktekens(t *testing.T) {
	m := parsePairs([]string{"GREETING=hallo, wereld", "EXPR=a=b=c", "zonder-is-teken"})
	if m["GREETING"] != "hallo, wereld" {
		t.Fatalf("GREETING=%q, wil %q", m["GREETING"], "hallo, wereld")
	}
	if m["EXPR"] != "a=b=c" {
		t.Fatalf("EXPR=%q, wil %q", m["EXPR"], "a=b=c")
	}
	if len(m) != 2 {
		t.Fatalf("map heeft %d entries, wil 2 (entry zonder '=' valt af)", len(m))
	}
}

func TestStringListVerzameltHerhaaldeFlags(t *testing.T) {
	var s stringList
	_ = s.Set("a=1")
	_ = s.Set("b=2")
	if len(s) != 2 || s[0] != "a=1" || s[1] != "b=2" {
		t.Fatalf("stringList = %v", s)
	}
}
