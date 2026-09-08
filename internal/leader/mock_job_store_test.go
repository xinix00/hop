package leader

import (
	"sync"
	"time"

	"github.com/xinix00/hop/internal/types"
)

func init() {
	// Fast timeouts for tests - no waiting for fake agents
	HTTPClientTimeout = 10 * time.Millisecond
	DeleteClientTimeout = 10 * time.Millisecond
	RollingUpdateDelay = 1 * time.Millisecond
}

// MockJobStore implements JobStore for testing
type MockJobStore struct {
	mu        sync.Mutex
	jobs      map[string]*types.Job
	stateTime time.Time
}

func NewMockJobStore() *MockJobStore {
	return &MockJobStore{
		jobs: make(map[string]*types.Job),
	}
}

func (m *MockJobStore) GetJobs() []*types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()

	jobs := make([]*types.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (m *MockJobStore) GetJob(name string) *types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[name]
}

func (m *MockJobStore) StoreJob(job *types.Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.Name] = job
}

func (m *MockJobStore) UpdateJob(job *types.Job) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[job.Name]; !ok {
		return false
	}
	m.jobs[job.Name] = job
	return true
}

func (m *MockJobStore) SetJobPriority(name string, priority int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.jobs[name]
	if !ok {
		return false
	}
	cp := *cur
	p := priority
	cp.Priority = &p
	m.jobs[name] = &cp
	return true
}

func (m *MockJobStore) SetJobDeploying(name string, deploying bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.jobs[name]
	if !ok {
		return false
	}
	cp := *cur
	cp.Deploying = deploying
	m.jobs[name] = &cp
	return true
}

func (m *MockJobStore) DeleteJob(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, name)
}

func (m *MockJobStore) GetStateTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stateTime
}

func (m *MockJobStore) SyncJobs(jobs []*types.Job, updated time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, job := range jobs {
		m.jobs[job.Name] = job
	}
	m.stateTime = updated
}
