// Command gitlab-ci-job-exporter reads finished CI jobs of a GitLab group and serves
// their durations, waiting times and results as Prometheus metrics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const (
	minInterval = 10 * time.Second
	// A long outage must not turn into one huge poll that GitLab has to page through.
	maxWindow        = 6 * time.Hour
	maxJobPages      = 20
	maxProjectPages  = 1000
	maxBodyBytes     = 32 << 20
	maxDrainBytes    = 1 << 20
	maxRetryAfter    = 30 * time.Second
	maxRetries       = 2
	maxPartLen       = 64
	maxTagsLen       = 128
	shutdownTimeout  = 5 * time.Second
	defaultSeriesTTL = 48 * time.Hour
)

type config struct {
	gitlabURL string
	token     string
	group     string
	interval  time.Duration
	lookback  time.Duration
	listen    string
	seriesTTL time.Duration
}

func parseDuration(name, def string) (time.Duration, error) {
	d, err := time.ParseDuration(env(name, def))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return d, nil
}

func validateGitLabURL(raw string, allowInsecure bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("GITLAB_URL %q is not a URL; use a form like https://gitlab.example.com", raw)
	}
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && allowInsecure:
	case u.Scheme == "http":
		return errors.New("GITLAB_URL uses http, which would send the token in clear text; use https or set ALLOW_INSECURE_HTTP=true")
	default:
		return fmt.Errorf("GITLAB_URL must start with https://, got scheme %q", u.Scheme)
	}
	if u.User != nil {
		return errors.New("GITLAB_URL must not contain credentials; put the token in GITLAB_TOKEN_FILE")
	}
	return nil
}

func validateToken(t string) error {
	if t == "" {
		return errors.New("the token file is empty; write the access token into it")
	}
	for _, r := range t {
		if unicode.IsControl(r) {
			return errors.New("the token file contains a control character after trimming; it must hold only the token")
		}
	}
	return nil
}

func loadConfig() (config, error) {
	c := config{
		gitlabURL: strings.TrimRight(env("GITLAB_URL", ""), "/"),
		group:     env("GITLAB_GROUP", ""),
		listen:    env("LISTEN", ":9168"),
	}
	var err error
	if c.interval, err = parseDuration("INTERVAL", "2m"); err != nil {
		return c, err
	}
	if c.interval < minInterval {
		return c, fmt.Errorf("INTERVAL %s is too short; use %s or more so GitLab is not hammered", c.interval, minInterval)
	}
	if c.lookback, err = parseDuration("LOOKBACK", "15m"); err != nil {
		return c, err
	}
	if c.lookback <= 0 {
		return c, errors.New("LOOKBACK must be greater than zero")
	}
	if c.seriesTTL, err = parseDuration("SERIES_TTL", defaultSeriesTTL.String()); err != nil {
		return c, err
	}
	if c.seriesTTL <= 0 {
		return c, errors.New("SERIES_TTL must be greater than zero")
	}
	tokenFile := env("GITLAB_TOKEN_FILE", "")
	if c.gitlabURL == "" || c.group == "" || tokenFile == "" {
		return c, errors.New("set GITLAB_URL, GITLAB_GROUP and GITLAB_TOKEN_FILE")
	}
	if err := validateGitLabURL(c.gitlabURL, os.Getenv("ALLOW_INSECURE_HTTP") == "true"); err != nil {
		return c, err
	}
	b, err := os.ReadFile(tokenFile)
	if err != nil {
		return c, fmt.Errorf("read the token: %w", err)
	}
	c.token = strings.TrimSpace(string(b))
	if err := validateToken(c.token); err != nil {
		return c, err
	}
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type project struct {
	ID                int    `json:"id"`
	PathWithNamespace string `json:"path_with_namespace"`
	DefaultBranch     string `json:"default_branch"`
}

type job struct {
	ID             int      `json:"id"`
	Name           string   `json:"name"`
	Status         string   `json:"status"`
	Ref            string   `json:"ref"`
	Duration       *float64 `json:"duration"`
	QueuedDuration *float64 `json:"queued_duration"`
	CreatedAt      string   `json:"created_at"`
	TagList        []string `json:"tag_list"`
	FinishedAt     string   `json:"finished_at"`
	Runner         *struct {
		Description string `json:"description"`
	} `json:"runner"`
	Pipeline struct {
		Source string `json:"source"`
	} `json:"pipeline"`
}

type pipeline struct {
	ID int `json:"id"`
}

type gitlab struct {
	base   string
	token  string
	client *http.Client
}

// newClient never follows a redirect: the token header would travel with it.
func newClient() *http.Client {
	return &http.Client{
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

type statusError struct {
	code   int
	status string
	path   string
}

func (e *statusError) Error() string { return fmt.Sprintf("GET %s: %s", e.path, e.status) }

func retryAfter(h http.Header) (time.Duration, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(h.Get("Retry-After")))
	if err != nil || n < 0 {
		return 0, false
	}
	return time.Duration(n) * time.Second, true
}

// fetch returns the body of one successful response, waiting out a short 429.
func (g gitlab) fetch(ctx context.Context, path string) ([]byte, http.Header, error) {
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.base+"/api/v4/"+path, nil)
		if err != nil {
			return nil, nil, err
		}
		req.Header.Set("PRIVATE-TOKEN", g.token)
		resp, err := g.client.Do(req)
		if err != nil {
			return nil, nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))
			resp.Body.Close()
			wait, ok := retryAfter(resp.Header)
			if resp.StatusCode == http.StatusTooManyRequests && ok && wait <= maxRetryAfter && attempt < maxRetries {
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-time.After(wait):
					continue
				}
			}
			return nil, nil, &statusError{code: resp.StatusCode, status: resp.Status, path: path}
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		resp.Body.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("GET %s: %w", path, err)
		}
		if len(body) > maxBodyBytes {
			return nil, nil, fmt.Errorf("GET %s: response larger than %d bytes", path, maxBodyBytes)
		}
		return body, resp.Header, nil
	}
}

// get decodes the pages of a list endpoint until stop says enough or maxPages
// is reached. The next page is built from X-Next-Page against the configured
// base, never from a URL the server sent, so the token stays on that host.
func get[T any](ctx context.Context, g gitlab, path string, maxPages int, stop func([]T) bool) (all []T, capped bool, err error) {
	next := path
	for page := 1; ; page++ {
		body, hdr, err := g.fetch(ctx, next)
		if err != nil {
			return nil, false, err
		}
		var items []T
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, false, fmt.Errorf("GET %s: %w", path, err)
		}
		all = append(all, items...)
		if stop != nil && stop(items) {
			return all, false, nil
		}
		np, err := strconv.Atoi(hdr.Get("X-Next-Page"))
		if err != nil || np <= 0 {
			return all, false, nil
		}
		if page >= maxPages {
			return all, true, nil
		}
		next = path + "&page=" + strconv.Itoa(np)
	}
}

// Buckets in seconds, from a short check to a stack job near its limit.
var buckets = []float64{30, 60, 120, 180, 300, 450, 600, 900, 1200, 1800, 2700, 3600}

type histogram struct {
	counts []uint64
	sum    float64
	count  uint64
	last   time.Time
}

func (h *histogram) observe(v float64, now time.Time) {
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
	h.last = now
}

type key struct{ project, job, part, runner, status, ref string }

type pendingKey struct{ project, tags string }

type pendingStat struct {
	count  int
	oldest time.Time
}

type store struct {
	mu              sync.Mutex
	now             func() time.Time
	pending         map[pendingKey]*pendingStat
	pipelines       map[string]map[string]int
	duration        map[key]*histogram
	queued          map[key]*histogram
	seen            map[int]time.Time
	lastPoll        time.Time
	lastPollSeconds float64
	pollError       float64
	projectsSkipped float64
	pageCapHits     float64
}

func newStore() *store {
	return &store{now: time.Now, pending: map[pendingKey]*pendingStat{}, pipelines: map[string]map[string]int{}, duration: map[key]*histogram{}, queued: map[key]*histogram{}, seen: map[int]time.Time{}}
}

// tagsLabel makes the same set of runner tags one label value whatever order GitLab lists them in.
func tagsLabel(tags []string) string {
	sorted := append([]string(nil), tags...)
	sort.Strings(sorted)
	return truncateRunes(strings.Join(sorted, ","), maxTagsLen)
}

func addPending(snap map[pendingKey]*pendingStat, projectPath string, j job) {
	k := pendingKey{projectPath, tagsLabel(j.TagList)}
	st := snap[k]
	if st == nil {
		st = &pendingStat{}
		snap[k] = st
	}
	st.count++
	if t, err := time.Parse(time.RFC3339, j.CreatedAt); err == nil && (st.oldest.IsZero() || t.Before(st.oldest)) {
		st.oldest = t
	}
}

// pipelineStatuses are the pipeline states exported as gauges. Pending means
// no job of the pipeline has found a runner yet.
var pipelineStatuses = []string{"pending", "running"}

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

func truncateRunes(v string, n int) string {
	if utf8.RuneCountInString(v) <= n {
		return v
	}
	return string([]rune(v)[:n])
}

func (s *store) add(projectPath string, j job, defaultRef string) {
	finished, err := time.Parse(time.RFC3339, j.FinishedAt)
	if err != nil {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[j.ID]; ok {
		return
	}
	s.seen[j.ID] = finished
	name, part := splitName(j.Name)
	part = truncateRunes(part, maxPartLen)
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
		v *float64
	}{{s.duration, j.Duration}, {s.queued, j.QueuedDuration}} {
		if o.v == nil {
			continue
		}
		h := o.m[k]
		if h == nil {
			h = &histogram{}
			o.m[k] = h
		}
		h.observe(*o.v, now)
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

// expire drops series that saw no job for ttl, so renamed jobs and runners
// do not pile up for the life of the process.
func (s *store) expire(now time.Time, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range []map[key]*histogram{s.duration, s.queued} {
		for k, h := range m {
			if now.Sub(h.last) > ttl {
				delete(m, k)
			}
		}
	}
}

func (s *store) count(c *float64) {
	s.mu.Lock()
	*c++
	s.mu.Unlock()
}

// jobTime is the job's finish time, or its creation time when GitLab has none.
func jobTime(j job) (time.Time, bool) {
	for _, v := range []string{j.FinishedAt, j.CreatedAt} {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func isStatus(err error, match func(code int) bool) bool {
	var se *statusError
	return errors.As(err, &se) && match(se.code)
}

func poll(ctx context.Context, g gitlab, group string, since time.Time, s *store, log *slog.Logger) error {
	projects, capped, err := get[project](ctx, g, "groups/"+url.PathEscape(group)+"/projects?include_subgroups=true&archived=false&per_page=100", maxProjectPages, nil)
	if err != nil {
		return err
	}
	if capped {
		return fmt.Errorf("the group has more than %d pages of projects", maxProjectPages)
	}
	older := func(page []job) bool {
		if len(page) == 0 {
			return true
		}
		for _, j := range page {
			if t, ok := jobTime(j); ok && t.Before(since) {
				return true
			}
		}
		return false
	}
	var errs []error
	snapshot := map[pendingKey]*pendingStat{}
	pipelineSnap := map[string]map[string]int{}
	for _, st := range pipelineStatuses {
		pipelineSnap[st] = map[string]int{}
	}
	for _, p := range projects {
		jobs, capped, err := get[job](ctx, g, fmt.Sprintf("projects/%d/jobs?scope[]=success&scope[]=failed&scope[]=canceled&per_page=100", p.ID), maxJobPages, older)
		if skip, fatal := classify(err, p, s, log, &errs); skip || fatal != nil {
			if fatal != nil {
				return fatal
			}
			continue
		}
		if capped {
			log.Warn("stopped paging a project at the page limit", "project", p.PathWithNamespace, "pages", maxJobPages)
			s.count(&s.pageCapHits)
		}
		for _, j := range jobs {
			if t, err := time.Parse(time.RFC3339, j.FinishedAt); err == nil && !t.Before(since) {
				s.add(p.PathWithNamespace, j, p.DefaultBranch)
			}
		}
		waiting, capped, err := get[job](ctx, g, fmt.Sprintf("projects/%d/jobs?scope[]=pending&per_page=100", p.ID), maxJobPages, nil)
		if skip, fatal := classify(err, p, s, log, &errs); skip || fatal != nil {
			if fatal != nil {
				return fatal
			}
			continue
		}
		if capped {
			log.Warn("stopped paging a project's pending jobs at the page limit", "project", p.PathWithNamespace, "pages", maxJobPages)
			s.count(&s.pageCapHits)
		}
		for _, j := range waiting {
			addPending(snapshot, p.PathWithNamespace, j)
		}
		for _, st := range pipelineStatuses {
			ps, capped, err := get[pipeline](ctx, g, fmt.Sprintf("projects/%d/pipelines?status=%s&per_page=100", p.ID, st), maxJobPages, nil)
			if skip, fatal := classify(err, p, s, log, &errs); skip || fatal != nil {
				if fatal != nil {
					return fatal
				}
				break
			}
			if capped {
				log.Warn("stopped paging a project's pipelines at the page limit", "project", p.PathWithNamespace, "status", st, "pages", maxJobPages)
				s.count(&s.pageCapHits)
			}
			if len(ps) > 0 {
				pipelineSnap[st][p.PathWithNamespace] = len(ps)
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	s.mu.Lock()
	s.pending = snapshot
	s.pipelines = pipelineSnap
	s.mu.Unlock()
	return nil
}

// classify sorts a failed project read: skip it, end the poll (fatal), or remember the error and go on.
func classify(err error, p project, s *store, log *slog.Logger, errs *[]error) (skip bool, fatal error) {
	switch {
	case err == nil:
		return false, nil
	case isStatus(err, func(c int) bool { return c == http.StatusForbidden || c == http.StatusNotFound }):
		log.Debug("skipping a project GitLab will not show", "project", p.PathWithNamespace, "err", err)
		s.count(&s.projectsSkipped)
		return true, nil
	case isStatus(err, func(c int) bool { return c == http.StatusTooManyRequests || c >= 500 }):
		return true, errors.Join(append(*errs, fmt.Errorf("%s: %w", p.PathWithNamespace, err))...)
	}
	*errs = append(*errs, fmt.Errorf("%s: %w", p.PathWithNamespace, err))
	return true, nil
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func label(k key) string {
	esc := func(v string) string { return labelEscaper.Replace(strings.ToValidUTF8(v, "\uFFFD")) }
	return fmt.Sprintf(`project="%s",job="%s",part="%s",runner="%s",status="%s",ref="%s"`,
		esc(k.project), esc(k.job), esc(k.part), esc(k.runner), esc(k.status), esc(k.ref))
}

func (s *store) writePending(w *strings.Builder) {
	keys := make([]pendingKey, 0, len(s.pending))
	for k := range s.pending {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].project != keys[j].project {
			return keys[i].project < keys[j].project
		}
		return keys[i].tags < keys[j].tags
	})
	esc := func(v string) string { return labelEscaper.Replace(strings.ToValidUTF8(v, "\uFFFD")) }
	now := s.now()
	fmt.Fprint(w, "# HELP ci_jobs_pending Jobs waiting for a runner at the last good poll.\n# TYPE ci_jobs_pending gauge\n")
	for _, k := range keys {
		fmt.Fprintf(w, "ci_jobs_pending{project=\"%s\",tags=\"%s\"} %d\n", esc(k.project), esc(k.tags), s.pending[k].count)
	}
	fmt.Fprint(w, "# HELP ci_job_pending_oldest_seconds Age of the oldest job waiting for a runner, counted from its creation, at scrape time.\n# TYPE ci_job_pending_oldest_seconds gauge\n")
	for _, k := range keys {
		if st := s.pending[k]; !st.oldest.IsZero() {
			fmt.Fprintf(w, "ci_job_pending_oldest_seconds{project=\"%s\",tags=\"%s\"} %g\n", esc(k.project), esc(k.tags), max(0, now.Sub(st.oldest).Seconds()))
		}
	}
}

func (s *store) writePipelines(w *strings.Builder) {
	esc := func(v string) string { return labelEscaper.Replace(strings.ToValidUTF8(v, "\uFFFD")) }
	for _, st := range pipelineStatuses {
		fmt.Fprintf(w, "# HELP ci_pipelines_%s Pipelines with status %s at the last good poll.\n# TYPE ci_pipelines_%s gauge\n", st, st, st)
		projects := make([]string, 0, len(s.pipelines[st]))
		for p := range s.pipelines[st] {
			projects = append(projects, p)
		}
		sort.Strings(projects)
		for _, p := range projects {
			fmt.Fprintf(w, "ci_pipelines_%s{project=\"%s\"} %d\n", st, esc(p), s.pipelines[st][p])
		}
	}
}

func (s *store) write(w *strings.Builder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writePending(w)
	s.writePipelines(w)
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
	var last int64
	if !s.lastPoll.IsZero() {
		last = s.lastPoll.Unix()
	}
	fmt.Fprintf(w, "# HELP ci_job_exporter_last_poll_timestamp_seconds When GitLab was last read without error; 0 before the first success.\n# TYPE ci_job_exporter_last_poll_timestamp_seconds gauge\nci_job_exporter_last_poll_timestamp_seconds %d\n", last)
	fmt.Fprintf(w, "# HELP ci_job_exporter_last_poll_duration_seconds How long the last finished poll took.\n# TYPE ci_job_exporter_last_poll_duration_seconds gauge\nci_job_exporter_last_poll_duration_seconds %g\n", s.lastPollSeconds)
	fmt.Fprintf(w, "# HELP ci_job_exporter_poll_errors_total Polls that failed for at least one project.\n# TYPE ci_job_exporter_poll_errors_total counter\nci_job_exporter_poll_errors_total %g\n", s.pollError)
	fmt.Fprintf(w, "# HELP ci_job_exporter_projects_skipped_total Project reads GitLab refused with 403 or 404.\n# TYPE ci_job_exporter_projects_skipped_total counter\nci_job_exporter_projects_skipped_total %g\n", s.projectsSkipped)
	fmt.Fprintf(w, "# HELP ci_job_exporter_page_limit_hits_total Project reads stopped at the page limit.\n# TYPE ci_job_exporter_page_limit_hits_total counter\nci_job_exporter_page_limit_hits_total %g\n", s.pageCapHits)
}

// windowStart keeps a poll from reaching further back than maxWindow.
func windowStart(since, now time.Time) time.Time {
	if floor := now.Add(-maxWindow); since.Before(floor) {
		return floor
	}
	return since
}

// advance moves the window only after a good poll. It then reaches back one
// interval, for jobs that finished while the poll ran; ids already seen are
// skipped. After a failure the window stays, so no job is lost between two
// good polls.
func advance(since, started time.Time, interval time.Duration, err error) time.Time {
	if err != nil {
		return since
	}
	return started.Add(-interval)
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
	g := gitlab{base: c.gitlabURL, token: c.token, client: newClient()}
	s := newStore()

	go func() {
		since := time.Now().Add(-c.lookback)
		for {
			started := time.Now()
			from := windowStart(since, started)
			err := poll(ctx, g, c.group, from, s, log)
			if ctx.Err() != nil {
				return
			}
			s.mu.Lock()
			s.lastPollSeconds = time.Since(started).Seconds()
			if err != nil {
				s.pollError++
			} else {
				s.lastPoll = started
			}
			s.mu.Unlock()
			if err != nil {
				log.Warn("reading GitLab failed", "err", err)
			}
			since = advance(since, started, c.interval, err)
			s.forget(from.Add(-time.Hour))
			s.expire(time.Now(), c.seriesTTL)
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
	srv := &http.Server{
		Addr:              c.listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("serving metrics", "listen", c.listen, "group", c.group)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serving metrics failed", "err", err)
		stop()
		<-shutdownDone
		os.Exit(1)
	}
	<-shutdownDone
}
