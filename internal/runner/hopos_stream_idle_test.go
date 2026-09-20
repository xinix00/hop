package runner

import (
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func TestStreamIdleTracksBytesBelowProgressBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pr.Close()
		defer pw.Close()
		idleErr := errors.New("download idle")
		const idle = 10 * time.Second
		timer := time.AfterFunc(idle, func() { pw.CloseWithError(idleErr) })
		defer timer.Stop()
		body := &meteredReader{r: pr, total: 1 << 20,
			tick: func(done, total uint64) { timer.Reset(idle) },
		}
		writerDone := make(chan struct{})
		defer func() { pr.Close(); pw.Close(); <-writerDone }()
		go func() {
			defer close(writerDone)
			chunk := make([]byte, 1024)
			for range 20 {
				time.Sleep(time.Second)
				if _, err := pw.Write(chunk); err != nil {
					return
				}
			}
			// Leave the connection open but silent: the same watchdog must now fire.
		}()
		start := time.Now()
		n, err := io.Copy(io.Discard, body)
		if n != 20*1024 {
			t.Fatalf("active slow download stopped after %d bytes: %v", n, err)
		}
		if !errors.Is(err, idleErr) {
			t.Fatalf("silent download not aborted: %v", err)
		}
		if elapsed := time.Since(start); elapsed != 20*time.Second+idle {
			t.Fatalf("idle measured from report instead of last byte: %s", elapsed)
		}
	})
}
