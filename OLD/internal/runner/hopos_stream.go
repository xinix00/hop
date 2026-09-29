package runner

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/hopos"
	"github.com/xinix00/lean/leanhttp"
)

// Dit bestand is het one-phase startpad: de node streamt het image van de
// artifact-URL rechtstreeks de partitie in (hopos.StreamStarter). Geen
// apploader, geen gestagede kopie — de partitie draagt alleen de app zelf, en
// de agent ziet de startfase als échte states (queued → downloading → running)
// in plaats van tien minuten "running" waarin niets draait.

// maxConcurrentDownloads begrenst hoeveel images de node tegelijk trekt. De
// grens is fysiek, geen beleid: elke stream kost de HOP-kern een TLS-sessie
// plus één leesbuffer, en TLS is op een klein board CPU-werk op de kern-core.
// Wie in de rij staat is gewoon "queued" — zichtbaar, en de capaciteit is al
// geteld.
const maxConcurrentDownloads = 4

// downloadStallTimeout is hoe lang een download STIL mag zijn — niet hoe lang
// hij mag duren. Zelfde les als de apploader (07-08): een 30MB-image over een
// gedeeld hart heeft de tijd nodig die hij nodig heeft; alleen een stroom
// zonder bytes is stuk.
const downloadStallTimeout = 60 * time.Second

// downloadHeaderTimeout is hoe lang een server mag zwijgen vóór de eerste byte.
// Vóór dat moment kan de stilte-bewaking niet werken: die reset op ontvangen
// data, en er is nog geen data.
const downloadHeaderTimeout = 60 * time.Second

// ProgressSink ontvangt de startfase-voortgang van een runner. De agent
// implementeert hem: hij zet de task op Downloading en draagt de bytes, zodat
// /tasks en de CLI laten zien wat er gebeurt.
type ProgressSink interface {
	TaskDownloading(taskID string, downloaded, total uint64)
}

// ProgressReporter is de optionele runner-kant: de agent injecteert zijn sink
// bij het opzetten. Runners zonder startfase (docker, exec vandaag) hoeven er
// niets van te weten.
type ProgressReporter interface {
	SetProgressSink(ProgressSink)
}

// SetProgressSink implementeert ProgressReporter.
func (r *HopRunner) SetProgressSink(s ProgressSink) {
	r.mu.Lock()
	r.progress = s
	r.mu.Unlock()
}

// runViaStream is de one-phase start. Contract naar Run: als (false, nil)
// terugkomt is de task tijdens de start gestopt en heeft Stop de opruiming;
// bij een fout is alles al vrijgegeven behalve de runner-boekhouding (de
// aanroeper released). Bij (true, nil) draait de app.
func (r *HopRunner) runViaStream(ctx context.Context, cancel context.CancelFunc, done chan struct{}, job *types.Job, task *types.Task, slot, cores int, sharegroup string, poolCores int, env map[string]string) (started bool, err error) {
	defer func() {
		cancel()
		close(done) // Stop wacht hierop: node-kant is nu opgeruimd of gearmd
	}()
	select {
	case <-ctx.Done():
		return false, nil
	default:
	}

	// De downloadbeurt. Wachten is zichtbaar (de task staat "queued") en
	// afbreekbaar (delete/stop tijdens de rij).
	select {
	case r.downloads <- struct{}{}:
		defer func() { <-r.downloads }()
	case <-ctx.Done():
		return false, nil
	}

	art := job.Artifacts[0]
	// Geen Timeout op de call: een image mag zo lang duren als hij duurt, de
	// stilte-bewaking hieronder is de grens. Wél een grens op de KOP: een server
	// die de verbinding aanneemt en dan zwijgt heeft nog geen byte gestuurd, dus
	// de stilte-bewaking is er nog niet en zou de download eeuwig laten hangen.
	// (Dit was net/http's ResponseHeaderTimeout op de gekloonde transport.)
	call := leanhttp.Call{
		Method:        leanhttp.MethodGet,
		URL:           art.URL,
		HeaderTimeout: downloadHeaderTimeout,
	}
	for k, v := range art.Headers {
		call.SetHeader(k, v)
	}
	if art.Auth["username"] != "" && art.Auth["password"] != "" {
		auth := base64.StdEncoding.EncodeToString(
			[]byte(art.Auth["username"] + ":" + art.Auth["password"]))
		call.SetHeader("Authorization", "Basic "+auth)
	}
	resp, err := artifactClient.DoContext(ctx, call)
	if err != nil {
		if ctx.Err() != nil {
			return false, nil // Stop brak de download af; die ruimt op
		}
		r.release(task.ID)
		return false, fmt.Errorf("hop driver: GET %s: %w", art.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != leanhttp.StatusOK {
		r.release(task.ID)
		return false, fmt.Errorf("hop driver: GET %s: HTTP %s", art.URL, resp.Status)
	}
	// Length is wat het transport BESLOTEN heeft te lezen, niet wat de header
	// beweert. Die twee lopen uiteen bij een chunked antwoord dat óók een
	// Content-Length draagt (zo ziet request smuggling eruit), en dan zou een
	// downloader die de header vertrouwt zijn bestand op de verkeerde maat zetten.
	size := resp.Length
	if size <= 0 {
		// Verplicht: de plaatsing valideert segmenten tegen de image-maat, en
		// een afgekapte stroom moet een luide fout zijn — niet een halve app.
		r.release(task.ID)
		return false, fmt.Errorf("hop driver: GET %s: no Content-Length", art.URL)
	}

	// Stilte-bewaking op de bodyfase + voortgang naar de agent.
	stall := time.AfterFunc(downloadStallTimeout, func() { resp.Body.Close() })
	defer stall.Stop()
	var lastReport uint64
	body := &meteredReader{
		r:     resp.Body,
		total: uint64(size),
		tick: func(done, total uint64) {
			stall.Reset(downloadStallTimeout)
			// Idle tracks every read; UI reporting remains batched.
			step := total / 100
			if step < 256<<10 {
				step = 256 << 10
			}
			if done-lastReport < step && done != total {
				return
			}
			lastReport = done
			if s := r.sink(); s != nil {
				s.TaskDownloading(task.ID, done, total)
			}
		},
	}
	if s := r.sink(); s != nil {
		s.TaskDownloading(task.ID, 0, uint64(size)) // queued → downloading
	}

	err = r.sm.StartStream(slot, body, size, hopos.StartSpec{
		MemLimit:   job.MemoryLimit,
		CoreClass:  job.Tags["core-class"],
		Cores:      cores,
		Sharegroup: sharegroup,
		PoolCores:  poolCores,
		Env:        env,
		Mounts:     job.Volumes,
		Ports:      task.Ports,
		Job:        job.Name,
	})
	if err != nil {
		if ctx.Err() != nil {
			return false, nil // Stop brak de download af; die ruimt op
		}
		r.release(task.ID)
		if !stall.Stop() {
			// De stilte-timer sloot de body: dat hoort er anders te staan dan
			// een kale leesfout op een gesloten stream.
			return false, fmt.Errorf("hop driver: slot %d: stalled after %d of %d bytes: no data for %s",
				slot, body.done, size, downloadStallTimeout)
		}
		return false, fmt.Errorf("hop driver: place stream slot %d: %w", slot, placementErr(err))
	}

	// Gearmd: een Stop vanaf nu is een gewone app-stop (sm.Stop), geen
	// download-abort meer.
	r.mu.Lock()
	r.armed[task.ID] = true
	stopping := r.stopping[task.ID]
	r.mu.Unlock()
	if stopping {
		return false, nil
	}
	log.Printf("hop driver: slot %d: streamed %d MB and started (job %s)", slot, size>>20, job.Name)
	return true, nil
}

func (r *HopRunner) sink() ProgressSink {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.progress
}

// meteredReader reports every positive read so idle tracking sees actual
// activity. The caller batches UI progress independently.
type meteredReader struct {
	r           io.Reader
	done, total uint64
	tick        func(done, total uint64)
}

func (m *meteredReader) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	if n > 0 {
		m.done += uint64(n)
		m.tick(m.done, m.total)
	}
	return n, err
}
