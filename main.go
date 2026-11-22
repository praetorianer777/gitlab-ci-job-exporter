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
	maxMRStateLen    = 32
	maxTestLen       = 128
	maxFailedCases   = 50
	maxTestSeries    = 2000
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
	// A pointer: GitLab leaves the field out when the project has issues disabled.
	OpenIssues *int `json:"open_issues_count"`
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
	// Retried is set by GitLab versions that mark a job replaced by a retry; the others leave it out.
	Retried bool `json:"retried"`
	Runner  *struct {
		Description string `json:"description"`
	} `json:"runner"`
	Pipeline struct {
		ID     int    `json:"id"`
		Source string `json:"source"`
	} `json:"pipeline"`
}

type pipeline struct {
	ID int `json:"id"`
}

// mergeRequest is one item of a merge request list. GitLab 15 and later send draft
// and detailed_merge_status; an older GitLab sends neither, so its states read "unknown".
type mergeRequest struct {
	ID        int    `json:"id"`
	CreatedAt string `json:"created_at"`
	MergedAt  string `json:"merged_at"`
	Draft     bool   `json:"draft"`
	Status    string `json:"detailed_merge_status"`
}

func (m mergeRequest) isDraft() bool { return m.Draft || m.Status == "draft_status" }

// finishedPipeline is one finished pipeline as the pipeline endpoint shows it;
// the list leaves out the times, so each one is read on its own.
type finishedPipeline struct {
	ID             int      `json:"id"`
	Status         string   `json:"status"`
	Ref            string   `json:"ref"`
	Source         string   `json:"source"`
	Tag            bool     `json:"tag"`
	CreatedAt      string   `json:"created_at"`
	FinishedAt     string   `json:"finished_at"`
	QueuedDuration *float64 `json:"queued_duration"`
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
	all, capped, _, err = getPages(ctx, g, path, maxPages, stop)
	return all, capped, err
}

// getPages is get that also returns the headers of the first page, which carry the totals.
func getPages[T any](ctx context.Context, g gitlab, path string, maxPages int, stop func([]T) bool) (all []T, capped bool, first http.Header, err error) {
	next := path
	for page := 1; ; page++ {
		body, hdr, err := g.fetch(ctx, next)
		if err != nil {
			return nil, false, nil, err
		}
		if page == 1 {
			first = hdr
		}
		var items []T
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, false, nil, fmt.Errorf("GET %s: %w", path, err)
		}
		all = append(all, items...)
		if stop != nil && stop(items) {
			return all, false, first, nil
		}
		np, err := strconv.Atoi(hdr.Get("X-Next-Page"))
		if err != nil || np <= 0 {
			return all, false, first, nil
		}
		if page >= maxPages {
			return all, true, first, nil
		}
		next = path + "&page=" + strconv.Itoa(np)
	}
}

// Buckets in seconds, from a short check to a stack job near its limit.
var buckets = []float64{30, 60, 120, 180, 300, 450, 600, 900, 1200, 1800, 2700, 3600}

// Pipelines run longer than single jobs: several stages, each waiting for a runner.
var pipelineBuckets = []float64{60, 180, 300, 600, 900, 1200, 1800, 2700, 3600, 5400, 7200, 10800}

// Merge requests wait for review: from ten minutes to two weeks.
var mergeBuckets = []float64{600, 1800, 3600, 7200, 14400, 28800, 43200, 86400, 172800, 345600, 604800, 1209600}

type histogram struct {
	counts []uint64
	sum    float64
	count  uint64
	last   time.Time
}

func (h *histogram) observe(v float64, now time.Time) {
	h.observeIn(buckets, v, now)
}

func (h *histogram) observeIn(bounds []float64, v float64, now time.Time) {
	if h.counts == nil {
		h.counts = make([]uint64, len(bounds))
	}
	for i, b := range bounds {
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

type pipelineKey struct{ project, status, ref string }

// seenPipeline tells a retried pipeline, which finishes again under the same id, from one already counted.
type seenPipeline struct {
	id       int
	finished string
}

type retryKey struct{ project, job, ref string }

type testKey struct{ project, suite, test string }

// counter is a monotonic count that expires with its series.
type counter struct {
	n    float64
	last time.Time
}

func bump[K comparable](m map[K]*counter, k K, now time.Time) {
	c := m[k]
	if c == nil {
		c = &counter{}
		m[k] = c
	}
	c.n++
	c.last = now
}

// attemptKey names one job of one pipeline: a retry runs under the same pipeline id and name.
type attemptKey struct {
	pipeline int
	name     string
}

// attempt is the newest run of a job seen so far, with what is needed to count it once a newer one shows up.
type attempt struct {
	id           int
	status       string
	rk           retryKey
	finished     time.Time
	failedBefore bool
	passed       bool
}

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
	pipeDuration    map[pipelineKey]*histogram
	pipeQueued      map[pipelineKey]*histogram
	pipeSeen        map[seenPipeline]time.Time
	defaultFailed   map[string]int
	openIssues      map[string]int
	openMRs         map[string]int
	mrOldest        map[string]time.Time
	mrStates        map[string]map[string]int
	mrMerge         map[string]*histogram
	mrSeen          map[int]time.Time
	retried         map[retryKey]*counter
	retryPassed     map[retryKey]*counter
	retriedSeen     map[int]time.Time
	attempts        map[attemptKey]*attempt
	testFailures    map[testKey]*counter
	lastPoll        time.Time
	lastPollSeconds float64
	pollError       float64
	projectsSkipped float64
	pageCapHits     float64
}

func newStore() *store {
	return &store{now: time.Now, pending: map[pendingKey]*pendingStat{}, pipelines: map[string]map[string]int{}, duration: map[key]*histogram{}, queued: map[key]*histogram{}, seen: map[int]time.Time{},
		pipeDuration: map[pipelineKey]*histogram{}, pipeQueued: map[pipelineKey]*histogram{}, pipeSeen: map[seenPipeline]time.Time{}, defaultFailed: map[string]int{}, openIssues: map[string]int{}, openMRs: map[string]int{},
		mrOldest: map[string]time.Time{}, mrStates: map[string]map[string]int{}, mrMerge: map[string]*histogram{}, mrSeen: map[int]time.Time{},
		retried: map[retryKey]*counter{}, retryPassed: map[retryKey]*counter{}, retriedSeen: map[int]time.Time{}, attempts: map[attemptKey]*attempt{}, testFailures: map[testKey]*counter{}}
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
	ref := refKind(j.Ref, j.Pipeline.Source, false, defaultRef)
	rk := retryKey{projectPath, name, ref}
	if j.Retried {
		s.markRetried(j.ID, rk, finished, now)
	}
	s.trackAttempt(j, rk, finished, now)
	// The histograms keep their meaning: only the run that was not replaced by a retry.
	if j.Retried {
		return
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

func (s *store) markRetried(id int, rk retryKey, finished, now time.Time) {
	if _, ok := s.retriedSeen[id]; ok {
		return
	}
	s.retriedSeen[id] = finished
	bump(s.retried, rk, now)
}

// trackAttempt finds retries by comparing runs of the same job in the same pipeline, since the
// older run is not necessarily marked and may have been read in an earlier poll. The caller holds the lock.
func (s *store) trackAttempt(j job, rk retryKey, finished, now time.Time) {
	if j.Pipeline.ID == 0 {
		return
	}
	ak := attemptKey{j.Pipeline.ID, j.Name}
	a := s.attempts[ak]
	if a == nil {
		s.attempts[ak] = &attempt{id: j.ID, status: j.Status, rk: rk, finished: finished}
		return
	}
	if j.ID < a.id {
		s.markRetried(j.ID, rk, finished, now)
		a.failedBefore = a.failedBefore || j.Status == "failed"
	} else {
		s.markRetried(a.id, a.rk, a.finished, now)
		a.failedBefore = a.failedBefore || a.status == "failed"
		a.id, a.status, a.rk, a.finished = j.ID, j.Status, rk, finished
	}
	if a.status == "success" && a.failedBefore && !a.passed {
		a.passed = true
		bump(s.retryPassed, a.rk, now)
	}
}

// refKind sorts a ref into default, merge_request, tag or branch.
func refKind(ref, source string, tag bool, defaultRef string) string {
	switch {
	case tag:
		return "tag"
	case ref == defaultRef:
		return "default"
	case strings.HasPrefix(ref, "refs/merge-requests/") || source == "merge_request_event":
		return "merge_request"
	}
	return "branch"
}

// addPipeline records a finished pipeline once per finish: its time from creation to the end, which is what
// someone waiting on it sees, and how long it waited before its first job started.
func (s *store) addPipeline(projectPath string, p finishedPipeline, defaultRef string) (added bool) {
	created, err1 := time.Parse(time.RFC3339, p.CreatedAt)
	finished, err2 := time.Parse(time.RFC3339, p.FinishedAt)
	if err1 != nil || err2 != nil {
		return false
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	sk := seenPipeline{p.ID, p.FinishedAt}
	if _, ok := s.pipeSeen[sk]; ok {
		return false
	}
	s.pipeSeen[sk] = finished
	k := pipelineKey{projectPath, p.Status, refKind(p.Ref, p.Source, p.Tag, defaultRef)}
	observe := func(m map[pipelineKey]*histogram, v float64) {
		h := m[k]
		if h == nil {
			h = &histogram{}
			m[k] = h
		}
		h.observeIn(pipelineBuckets, v, now)
	}
	observe(s.pipeDuration, max(0, finished.Sub(created).Seconds()))
	if p.QueuedDuration != nil {
		observe(s.pipeQueued, *p.QueuedDuration)
	}
	return true
}

// addMerged records a merge request once, by id, from its creation to its merge. A merge
// request merged before the window start was counted by an earlier poll or predates the exporter.
func (s *store) addMerged(projectPath string, m mergeRequest, since time.Time) {
	created, err1 := time.Parse(time.RFC3339, m.CreatedAt)
	merged, err2 := time.Parse(time.RFC3339, m.MergedAt)
	if err1 != nil || err2 != nil || merged.Before(since) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.mrSeen[m.ID]; ok {
		return
	}
	s.mrSeen[m.ID] = merged
	h := s.mrMerge[projectPath]
	if h == nil {
		h = &histogram{}
		s.mrMerge[projectPath] = h
	}
	h.observeIn(mergeBuckets, max(0, merged.Sub(created).Seconds()), time.Now())
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
	for k, t := range s.pipeSeen {
		if t.Before(before) {
			delete(s.pipeSeen, k)
		}
	}
	for id, t := range s.mrSeen {
		if t.Before(before) {
			delete(s.mrSeen, id)
		}
	}
	for id, t := range s.retriedSeen {
		if t.Before(before) {
			delete(s.retriedSeen, id)
		}
	}
	for k, a := range s.attempts {
		if a.finished.Before(before) {
			delete(s.attempts, k)
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
	for _, m := range []map[pipelineKey]*histogram{s.pipeDuration, s.pipeQueued} {
		for k, h := range m {
			if now.Sub(h.last) > ttl {
				delete(m, k)
			}
		}
	}
	for k, h := range s.mrMerge {
		if now.Sub(h.last) > ttl {
			delete(s.mrMerge, k)
		}
	}
	for _, m := range []map[retryKey]*counter{s.retried, s.retryPassed} {
		for k, c := range m {
			if now.Sub(c.last) > ttl {
				delete(m, k)
			}
		}
	}
	for k, c := range s.testFailures {
		if now.Sub(c.last) > ttl {
			delete(s.testFailures, k)
		}
	}
}

// addTestFailures counts the failed cases of one pipeline, at most maxFailedCases of them. A
// series beyond maxTestSeries is dropped, so a broken suite cannot grow the exporter without bound.
func (s *store) addTestFailures(projectPath string, r testReport) (dropped int) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	taken := 0
	for _, suite := range r.TestSuites {
		for _, c := range suite.TestCases {
			if c.Status != "failed" && c.Status != "error" {
				continue
			}
			if taken >= maxFailedCases {
				return dropped
			}
			taken++
			name := c.Name
			if c.Classname != "" {
				name = c.Classname + "." + c.Name
			}
			k := testKey{projectPath, truncateRunes(suite.Name, maxTestLen), truncateRunes(name, maxTestLen)}
			if _, ok := s.testFailures[k]; !ok && len(s.testFailures) >= maxTestSeries {
				dropped++
				continue
			}
			bump(s.testFailures, k, now)
		}
	}
	return dropped
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
	defaultSnap := map[string]int{}
	issueSnap := map[string]int{}
	mrSnap := map[string]int{}
	oldestSnap := map[string]time.Time{}
	stateSnap := map[string]map[string]int{}
	pipelineSnap := map[string]map[string]int{}
	for _, st := range pipelineStatuses {
		pipelineSnap[st] = map[string]int{}
	}
	for _, p := range projects {
		// The count comes with the project list, so a project whose other reads are refused still has one.
		if p.OpenIssues != nil {
			issueSnap[p.PathWithNamespace] = *p.OpenIssues
		}
		jobs, capped, err := get[job](ctx, g, fmt.Sprintf("projects/%d/jobs?scope[]=success&scope[]=failed&scope[]=canceled&include_retried=true&per_page=100", p.ID), maxJobPages, older)
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
		skipped := false
		for _, st := range pipelineStatuses {
			ps, capped, err := get[pipeline](ctx, g, fmt.Sprintf("projects/%d/pipelines?status=%s&per_page=100", p.ID, st), maxJobPages, nil)
			if skip, fatal := classify(err, p, s, log, &errs); skip || fatal != nil {
				if fatal != nil {
					return fatal
				}
				skipped = true
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
		if skipped {
			continue
		}
		if fatal := pollPipelines(ctx, g, p, since, s, log, &errs, defaultSnap); fatal != nil {
			return fatal
		}
		flow, err := openMergeRequests(ctx, g, p, s, log)
		if skip, fatal := classify(err, p, s, log, &errs); skip || fatal != nil {
			if fatal != nil {
				return fatal
			}
			continue
		}
		if flow.hasTotal {
			mrSnap[p.PathWithNamespace] = flow.total
		}
		if !flow.oldest.IsZero() {
			oldestSnap[p.PathWithNamespace] = flow.oldest
		}
		if flow.byState != nil {
			stateSnap[p.PathWithNamespace] = flow.byState
		}
		if fatal := pollMerged(ctx, g, p, since, s, log, &errs); fatal != nil {
			return fatal
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	s.mu.Lock()
	s.pending = snapshot
	s.pipelines = pipelineSnap
	s.defaultFailed = defaultSnap
	s.openIssues = issueSnap
	s.openMRs = mrSnap
	s.mrOldest = oldestSnap
	s.mrStates = stateSnap
	s.mu.Unlock()
	return nil
}

// pollPipelines records the pipelines that finished since the window start, and whether the
// newest finished pipeline of the default branch failed.
func pollPipelines(ctx context.Context, g gitlab, p project, since time.Time, s *store, log *slog.Logger, errs *[]error, defaultSnap map[string]int) error {
	list, capped, err := get[pipeline](ctx, g, fmt.Sprintf("projects/%d/pipelines?scope=finished&updated_after=%s&order_by=updated_at&sort=desc&per_page=100",
		p.ID, url.QueryEscape(since.UTC().Format(time.RFC3339))), maxJobPages, nil)
	if skip, fatal := classify(err, p, s, log, errs); skip || fatal != nil {
		return fatal
	}
	if capped {
		log.Warn("stopped paging a project's finished pipelines at the page limit", "project", p.PathWithNamespace, "pages", maxJobPages)
		s.count(&s.pageCapHits)
	}
	reports := true
	for _, item := range list {
		body, _, err := g.fetch(ctx, fmt.Sprintf("projects/%d/pipelines/%d", p.ID, item.ID))
		var fp finishedPipeline
		if err == nil {
			if err = json.Unmarshal(body, &fp); err != nil {
				err = fmt.Errorf("pipeline %d: %w", item.ID, err)
			}
		}
		if skip, fatal := classify(err, p, s, log, errs); skip || fatal != nil {
			if fatal != nil {
				return fatal
			}
			continue
		}
		if t, err := time.Parse(time.RFC3339, fp.FinishedAt); err == nil && !t.Before(since) &&
			s.addPipeline(p.PathWithNamespace, fp, p.DefaultBranch) && fp.Status == "failed" && reports {
			var fatal error
			if reports, fatal = pollTestReport(ctx, g, p, fp.ID, s, log, errs); fatal != nil {
				return fatal
			}
		}
	}
	if p.DefaultBranch == "" {
		return nil
	}
	latest, _, err := get[finishedPipeline](ctx, g, fmt.Sprintf("projects/%d/pipelines?scope=finished&ref=%s&order_by=id&sort=desc&per_page=1",
		p.ID, url.QueryEscape(p.DefaultBranch)), 1, nil)
	if skip, fatal := classify(err, p, s, log, errs); skip || fatal != nil {
		return fatal
	}
	if len(latest) > 0 {
		failed := 0
		if latest[0].Status == "failed" {
			failed = 1
		}
		defaultSnap[p.PathWithNamespace] = failed
	}
	return nil
}

// mrFlow is what one read of a project's open merge requests tells.
type mrFlow struct {
	total    int
	hasTotal bool
	oldest   time.Time
	byState  map[string]int
}

type testSummary struct {
	Total struct {
		Count int `json:"count"`
	} `json:"total"`
}

type testReport struct {
	TestSuites []struct {
		Name      string `json:"name"`
		TestCases []struct {
			Status    string `json:"status"`
			Name      string `json:"name"`
			Classname string `json:"classname"`
		} `json:"test_cases"`
	} `json:"test_suites"`
}

func getJSON[T any](ctx context.Context, g gitlab, path string) (v T, err error) {
	body, _, err := g.fetch(ctx, path)
	if err != nil {
		return v, err
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return v, fmt.Errorf("GET %s: %w", path, err)
	}
	return v, nil
}

// pollTestReport counts the failed test cases of one failed pipeline. The small summary comes
// first, so a pipeline without a report costs one read. more is false once GitLab refuses the
// reads, so the project's other failed pipelines do not ask again in this poll.
func pollTestReport(ctx context.Context, g gitlab, p project, pipelineID int, s *store, log *slog.Logger, errs *[]error) (more bool, fatal error) {
	base := fmt.Sprintf("projects/%d/pipelines/%d/", p.ID, pipelineID)
	sum, err := getJSON[testSummary](ctx, g, base+"test_report_summary")
	if skip, fatal := classify(err, p, s, log, errs); skip || fatal != nil {
		return false, fatal
	}
	if sum.Total.Count == 0 {
		return true, nil
	}
	rep, err := getJSON[testReport](ctx, g, base+"test_report")
	if skip, fatal := classify(err, p, s, log, errs); skip || fatal != nil {
		return false, fatal
	}
	if n := s.addTestFailures(p.PathWithNamespace, rep); n > 0 {
		log.Warn("left out failed test cases over the limit", "project", p.PathWithNamespace, "pipeline", pipelineID, "left_out", n)
	}
	return true, nil
}

// openMergeRequests pages through the open merge requests, oldest first, so the oldest one that is
// not a draft is near the front even when the page cap cuts the list short. A capped list gives
// no state counts, since they would be too low; the total then comes from the X-Total header,
// which GitLab leaves out above 10000 items, and without it the project has no count series.
func openMergeRequests(ctx context.Context, g gitlab, p project, s *store, log *slog.Logger) (f mrFlow, err error) {
	list, capped, hdr, err := getPages[mergeRequest](ctx, g, fmt.Sprintf("projects/%d/merge_requests?state=opened&order_by=created_at&sort=asc&per_page=100", p.ID), maxJobPages, nil)
	if err != nil {
		return f, err
	}
	if capped {
		log.Warn("stopped paging a project's open merge requests at the page limit", "project", p.PathWithNamespace, "pages", maxJobPages)
		s.count(&s.pageCapHits)
	}
	if n, err := strconv.Atoi(hdr.Get("X-Total")); err == nil && n >= 0 {
		f.total, f.hasTotal = n, true
	} else if !capped {
		f.total, f.hasTotal = len(list), true
	} else {
		log.Warn("GitLab sent no total of open merge requests, so the project has no series", "project", p.PathWithNamespace)
	}
	if !capped {
		f.byState = map[string]int{}
	}
	for _, m := range list {
		if f.byState != nil {
			st := m.Status
			if st == "" {
				st = "unknown"
			}
			f.byState[truncateRunes(st, maxMRStateLen)]++
		}
		if t, err := time.Parse(time.RFC3339, m.CreatedAt); err == nil && !m.isDraft() && (f.oldest.IsZero() || t.Before(f.oldest)) {
			f.oldest = t
		}
	}
	return f, nil
}

// pollMerged records the merge requests merged since the window start.
func pollMerged(ctx context.Context, g gitlab, p project, since time.Time, s *store, log *slog.Logger, errs *[]error) error {
	list, capped, err := get[mergeRequest](ctx, g, fmt.Sprintf("projects/%d/merge_requests?state=merged&updated_after=%s&order_by=updated_at&sort=desc&per_page=100",
		p.ID, url.QueryEscape(since.UTC().Format(time.RFC3339))), maxJobPages, nil)
	if skip, fatal := classify(err, p, s, log, errs); skip || fatal != nil {
		return fatal
	}
	if capped {
		log.Warn("stopped paging a project's merged merge requests at the page limit", "project", p.PathWithNamespace, "pages", maxJobPages)
		s.count(&s.pageCapHits)
	}
	for _, m := range list {
		s.addMerged(p.PathWithNamespace, m, since)
	}
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

func (s *store) writeOpenCounts(w *strings.Builder) {
	esc := func(v string) string { return labelEscaper.Replace(strings.ToValidUTF8(v, "\uFFFD")) }
	for _, m := range []struct {
		name, help string
		counts     map[string]int
	}{
		{"ci_project_open_issues", "Open issues of a project at the last good poll; no series when the project has issues disabled.", s.openIssues},
		{"ci_project_open_merge_requests", "Open merge requests of a project at the last good poll; no series when GitLab sent no total.", s.openMRs},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", m.name, m.help, m.name)
		projects := make([]string, 0, len(m.counts))
		for p := range m.counts {
			projects = append(projects, p)
		}
		sort.Strings(projects)
		for _, p := range projects {
			fmt.Fprintf(w, "%s{project=\"%s\"} %d\n", m.name, esc(p), m.counts[p])
		}
	}
}

func (s *store) writeMergeRequests(w *strings.Builder) {
	projects := make([]string, 0, len(s.mrOldest))
	for p := range s.mrOldest {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	now := s.now()
	fmt.Fprint(w, "# HELP ci_merge_request_oldest_open_seconds Age of the oldest open merge request that is not a draft, counted from its creation, at scrape time.\n# TYPE ci_merge_request_oldest_open_seconds gauge\n")
	for _, p := range projects {
		fmt.Fprintf(w, "ci_merge_request_oldest_open_seconds{project=\"%s\"} %g\n", escLabel(p), max(0, now.Sub(s.mrOldest[p]).Seconds()))
	}
	fmt.Fprint(w, "# HELP ci_merge_requests_open_by_state Open merge requests by GitLab's detailed merge status at the last good poll; no series for a project whose list hit the page limit.\n# TYPE ci_merge_requests_open_by_state gauge\n")
	projects = projects[:0]
	for p := range s.mrStates {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		states := make([]string, 0, len(s.mrStates[p]))
		for st := range s.mrStates[p] {
			states = append(states, st)
		}
		sort.Strings(states)
		for _, st := range states {
			fmt.Fprintf(w, "ci_merge_requests_open_by_state{project=\"%s\",state=\"%s\"} %d\n", escLabel(p), escLabel(st), s.mrStates[p][st])
		}
	}
	const name = "ci_merge_request_time_to_merge_seconds"
	fmt.Fprintf(w, "# HELP %s How long a merge request took from its creation to its merge.\n# TYPE %s histogram\n", name, name)
	projects = projects[:0]
	for p := range s.mrMerge {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		h, l := s.mrMerge[p], fmt.Sprintf(`project="%s"`, escLabel(p))
		for i, b := range mergeBuckets {
			fmt.Fprintf(w, "%s_bucket{%s,le=\"%g\"} %d\n", name, l, b, h.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n%s_sum{%s} %g\n%s_count{%s} %d\n", name, l, h.count, name, l, h.sum, name, l, h.count)
	}
}

func (s *store) writeCounters(w *strings.Builder) {
	for _, m := range []struct {
		name, help string
		c          map[retryKey]*counter
	}{
		{"ci_job_retried_total", "Finished jobs that were later retried, each job run counted once.", s.retried},
		{"ci_job_retry_passed_total", "Jobs that failed and whose retry in the same pipeline succeeded, counted once per job and pipeline.", s.retryPassed},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", m.name, m.help, m.name)
		keys := make([]retryKey, 0, len(m.c))
		for k := range m.c {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
		for _, k := range keys {
			fmt.Fprintf(w, "%s{project=\"%s\",job=\"%s\",ref=\"%s\"} %g\n", m.name, escLabel(k.project), escLabel(k.job), escLabel(k.ref), m.c[k].n)
		}
	}
	fmt.Fprint(w, "# HELP ci_test_failures_total Failed test cases of failed pipelines, from their test reports, each pipeline counted once.\n# TYPE ci_test_failures_total counter\n")
	keys := make([]testKey, 0, len(s.testFailures))
	for k := range s.testFailures {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
	for _, k := range keys {
		fmt.Fprintf(w, "ci_test_failures_total{project=\"%s\",suite=\"%s\",test=\"%s\"} %g\n", escLabel(k.project), escLabel(k.suite), escLabel(k.test), s.testFailures[k].n)
	}
}

func escLabel(v string) string { return labelEscaper.Replace(strings.ToValidUTF8(v, "\uFFFD")) }

func pipelineLabel(k pipelineKey) string {
	esc := func(v string) string { return labelEscaper.Replace(strings.ToValidUTF8(v, "\uFFFD")) }
	return fmt.Sprintf(`project="%s",status="%s",ref="%s"`, esc(k.project), esc(k.status), esc(k.ref))
}

func (s *store) writeFinishedPipelines(w *strings.Builder) {
	for _, m := range []struct {
		name, help string
		h          map[pipelineKey]*histogram
	}{
		{"ci_pipeline_duration_seconds", "How long a finished pipeline took from its creation to its end.", s.pipeDuration},
		{"ci_pipeline_queued_seconds", "How long a finished pipeline waited before its first job started.", s.pipeQueued},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", m.name, m.help, m.name)
		keys := make([]pipelineKey, 0, len(m.h))
		for k := range m.h {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return pipelineLabel(keys[i]) < pipelineLabel(keys[j]) })
		for _, k := range keys {
			h, l := m.h[k], pipelineLabel(k)
			for i, b := range pipelineBuckets {
				fmt.Fprintf(w, "%s_bucket{%s,le=\"%g\"} %d\n", m.name, l, b, h.counts[i])
			}
			fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n%s_sum{%s} %g\n%s_count{%s} %d\n", m.name, l, h.count, m.name, l, h.sum, m.name, l, h.count)
		}
	}
	esc := func(v string) string { return labelEscaper.Replace(strings.ToValidUTF8(v, "\uFFFD")) }
	fmt.Fprint(w, "# HELP ci_default_branch_failed 1 when the newest finished pipeline of the default branch failed, at the last good poll.\n# TYPE ci_default_branch_failed gauge\n")
	projects := make([]string, 0, len(s.defaultFailed))
	for p := range s.defaultFailed {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		fmt.Fprintf(w, "ci_default_branch_failed{project=\"%s\"} %d\n", esc(p), s.defaultFailed[p])
	}
}

func (s *store) write(w *strings.Builder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writePending(w)
	s.writePipelines(w)
	s.writeOpenCounts(w)
	s.writeMergeRequests(w)
	s.writeCounters(w)
	s.writeFinishedPipelines(w)
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
