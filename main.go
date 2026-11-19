// Command gitlab-ci-job-exporter reads finished CI jobs of a GitLab group and serves
// their durations, waiting times and results as Prometheus metrics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

type config struct {
	gitlabURL string
	token     string
	group     string
	interval  time.Duration
	lookback  time.Duration
	listen    string
}

func loadConfig() (config, error) {
	c := config{
		gitlabURL: strings.TrimRight(env("GITLAB_URL", ""), "/"),
		group:     env("GITLAB_GROUP", ""),
		listen:    env("LISTEN", ":9168"),
	}
	var err error
	if c.interval, err = time.ParseDuration(env("INTERVAL", "2m")); err != nil {
		return c, fmt.Errorf("INTERVAL: %w", err)
	}
	if c.lookback, err = time.ParseDuration(env("LOOKBACK", "15m")); err != nil {
		return c, fmt.Errorf("LOOKBACK: %w", err)
	}
	tokenFile := env("GITLAB_TOKEN_FILE", "")
	if c.gitlabURL == "" || c.group == "" || tokenFile == "" {
		return c, errors.New("set GITLAB_URL, GITLAB_GROUP and GITLAB_TOKEN_FILE")
	}
	b, err := os.ReadFile(tokenFile)
	if err != nil {
		return c, fmt.Errorf("read the token: %w", err)
	}
	c.token = strings.TrimSpace(string(b))
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type project struct {
	ID            int    `json:"id"`
	Path          string `json:"path"`
	DefaultBranch string `json:"default_branch"`
}

type job struct {
	ID             int     `json:"id"`
	Name           string  `json:"name"`
	Status         string  `json:"status"`
	Ref            string  `json:"ref"`
	Duration       float64 `json:"duration"`
	QueuedDuration float64 `json:"queued_duration"`
	FinishedAt     string  `json:"finished_at"`
	Runner         *struct {
		Description string `json:"description"`
	} `json:"runner"`
	Pipeline struct {
		Source string `json:"source"`
	} `json:"pipeline"`
}

type gitlab struct {
	base   string
	token  string
	client *http.Client
}

var nextPage = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

// get decodes every page of a list endpoint until stop says enough.
func get[T any](ctx context.Context, g gitlab, path string, stop func([]T) bool) ([]T, error) {
	var all []T
	next := g.base + "/api/v4/" + path
	for next != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, next, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("PRIVATE-TOKEN", g.token)
		resp, err := g.client.Do(req)
		if err != nil {
			return nil, err
		}
		var page []T
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GET %s: %s", path, resp.Status)
		}
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", path, err)
		}
		all = append(all, page...)
		if stop != nil && stop(page) {
			break
		}
		next = ""
		if m := nextPage.FindStringSubmatch(resp.Header.Get("Link")); m != nil {
			next = m[1]
		}
	}
	return all, nil
}

// Buckets in seconds, from a short check to a stack job near its limit.
var buckets = []float64{30, 60, 120, 180, 300, 450, 600, 900, 1200, 1800, 2700, 3600}

type histogram struct {
	counts []uint64
	sum    float64
	count  uint64
}

func (h *histogram) observe(v float64) {
	if h.counts == nil {
		h.counts = make([]uint64, len(buckets))
	}
	for i, b := range buckets {
		if v <= b {
			h.counts[i]++
		}
	}
	h.sum += v
	h.count++
}

type key struct{ project, job, part, runner, status, ref string }

type store struct {
	mu        sync.Mutex
	duration  map[key]*histogram
	queued    map[key]*histogram
	seen      map[int]time.Time
	lastPoll  time.Time
	pollError float64
}

func newStore() *store {
	return &store{duration: map[key]*histogram{}, queued: map[key]*histogram{}, seen: map[int]time.Time{}}
}

var parallelPart = regexp.MustCompile(`^(.*?)(?::\s*\[(.*)\]|\s+(\d+/\d+))$`)

// splitName turns "test-e2e: [1/2]" into ("test-e2e", "1/2").
func splitName(n string) (string, string) {
	m := parallelPart.FindStringSubmatch(n)
	if m == nil {
		return n, ""
	}
	if m[2] != "" {
		return strings.TrimSpace(m[1]), m[2]
	}
	return strings.TrimSpace(m[1]), m[3]
}

func (s *store) add(projectPath string, j job, defaultRef string) {
	finished, err := time.Parse(time.RFC3339, j.FinishedAt)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[j.ID]; ok {
		return
	}
	s.seen[j.ID] = finished
	name, part := splitName(j.Name)
	runner := "none"
	if j.Runner != nil && j.Runner.Description != "" {
		runner = j.Runner.Description
	}
	ref := "branch"
	if j.Ref == defaultRef {
		ref = "default"
	} else if strings.HasPrefix(j.Ref, "refs/merge-requests/") || j.Pipeline.Source == "merge_request_event" {
		ref = "merge_request"
	}
	k := key{projectPath, name, part, runner, j.Status, ref}
	for _, o := range []struct {
		m map[key]*histogram
		v float64
	}{{s.duration, j.Duration}, {s.queued, j.QueuedDuration}} {
		h := o.m[k]
		if h == nil {
			h = &histogram{}
			o.m[k] = h
		}
		h.observe(o.v)
	}
}

// forget drops seen job ids older than the window a poll can return.
func (s *store) forget(before time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.seen {
		if t.Before(before) {
			delete(s.seen, id)
		}
	}
}

func poll(ctx context.Context, g gitlab, group string, since time.Time, s *store) error {
	projects, err := get[project](ctx, g, "groups/"+url.PathEscape(group)+"/projects?include_subgroups=true&archived=false&per_page=100", nil)
	if err != nil {
		return err
	}
	var errs []error
	for _, p := range projects {
		older := func(page []job) bool {
			if len(page) == 0 {
				return true
			}
			t, err := time.Parse(time.RFC3339, page[len(page)-1].FinishedAt)
			return err == nil && t.Before(since)
		}
		jobs, err := get[job](ctx, g, fmt.Sprintf("projects/%d/jobs?scope[]=success&scope[]=failed&scope[]=canceled&per_page=100", p.ID), older)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", p.Path, err))
			continue
		}
		for _, j := range jobs {
			if t, err := time.Parse(time.RFC3339, j.FinishedAt); err == nil && !t.Before(since) {
				s.add(p.Path, j, p.DefaultBranch)
			}
		}
	}
	return errors.Join(errs...)
}

func label(k key) string {
	esc := func(v string) string { return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v) }
	return fmt.Sprintf(`project="%s",job="%s",part="%s",runner="%s",status="%s",ref="%s"`,
		esc(k.project), esc(k.job), esc(k.part), esc(k.runner), esc(k.status), esc(k.ref))
}

func (s *store) write(w *strings.Builder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range []struct {
		name, help string
		h          map[key]*histogram
	}{
		{"ci_job_duration_seconds", "How long a finished CI job ran.", s.duration},
		{"ci_job_queued_seconds", "How long a finished CI job waited for a runner.", s.queued},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", m.name, m.help, m.name)
		keys := make([]key, 0, len(m.h))
		for k := range m.h {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return label(keys[i]) < label(keys[j]) })
		for _, k := range keys {
			h, l := m.h[k], label(k)
			for i, b := range buckets {
				fmt.Fprintf(w, "%s_bucket{%s,le=\"%g\"} %d\n", m.name, l, b, h.counts[i])
			}
			fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n%s_sum{%s} %g\n%s_count{%s} %d\n", m.name, l, h.count, m.name, l, h.sum, m.name, l, h.count)
		}
	}
	fmt.Fprintf(w, "# HELP ci_job_exporter_build_info Version of the running exporter.\n# TYPE ci_job_exporter_build_info gauge\nci_job_exporter_build_info{version=\"%s\"} 1\n", version)
	fmt.Fprintf(w, "# HELP ci_job_exporter_last_poll_timestamp_seconds When GitLab was last read without error.\n# TYPE ci_job_exporter_last_poll_timestamp_seconds gauge\nci_job_exporter_last_poll_timestamp_seconds %d\n", s.lastPoll.Unix())
	fmt.Fprintf(w, "# HELP ci_job_exporter_poll_errors_total Polls that failed for at least one project.\n# TYPE ci_job_exporter_poll_errors_total counter\nci_job_exporter_poll_errors_total %g\n", s.pollError)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	log.Info("starting", "version", version)
	c, err := loadConfig()
	if err != nil {
		log.Error("cannot start", "err", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g := gitlab{base: c.gitlabURL, token: c.token, client: &http.Client{Timeout: 30 * time.Second}}
	s := newStore()

	go func() {
		since := time.Now().Add(-c.lookback)
		for {
			started := time.Now()
			err := poll(ctx, g, c.group, since, s)
			s.mu.Lock()
			if err != nil {
				s.pollError++
			} else {
				s.lastPoll = started
			}
			s.mu.Unlock()
			if err != nil {
				log.Warn("reading GitLab failed", "err", err)
			}
			// The next poll looks back over this one too, for jobs that finished
			// while it ran; ids already seen are skipped.
			since = started.Add(-c.interval)
			s.forget(since.Add(-time.Hour))
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.interval):
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		var b strings.Builder
		s.write(&b)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(b.String()))
	})
	srv := &http.Server{Addr: c.listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Info("serving metrics", "listen", c.listen, "group", c.group)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serving metrics failed", "err", err)
		os.Exit(1)
	}
}
