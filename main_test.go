package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestSplitNameSeparatesParallelParts(t *testing.T) {
	for in, want := range map[string][2]string{
		"test-e2e: [1/2]": {"test-e2e", "1/2"},
		"test-integration: [2/3, test-integration]": {"test-integration", "2/3, test-integration"},
		"test-e2e 1/2":              {"test-e2e", "1/2"},
		"check-go":                  {"check-go", ""},
		"chart install and upgrade": {"chart install and upgrade", ""},
	} {
		if n, p := splitName(in); n != want[0] || p != want[1] {
			t.Errorf("splitName(%q) = %q, %q; want %q, %q", in, n, p, want[0], want[1])
		}
	}
}

// fakeGitLab serves one project with jobs on two pages, newest first, and
// counts the requests so a test can see where paging stopped.
func fakeGitLab(t *testing.T, now time.Time) (*httptest.Server, *int) {
	pages := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/my-group/projects", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("PRIVATE-TOKEN") != "secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		fmt.Fprint(w, `[{"id":69,"path_with_namespace":"my-group/my-project","default_branch":"main"}]`)
	})
	mux.HandleFunc("/api/v4/projects/69/pipelines", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[]`)
	})
	mux.HandleFunc("/api/v4/projects/69/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope[]") == "pending" {
			fmt.Fprint(w, `[]`)
			return
		}
		pages++
		f := func(d time.Duration) string { return now.Add(-d).UTC().Format(time.RFC3339) }
		if r.URL.Query().Get("page") == "2" {
			fmt.Fprintf(w, `[{"id":3,"name":"check-go","status":"success","ref":"main","duration":100,"queued_duration":1,"finished_at":%q}]`, f(3*time.Hour))
			return
		}
		w.Header().Set("X-Next-Page", "2")
		fmt.Fprintf(w, `[
		  {"id":1,"name":"test-e2e: [1/2]","status":"failed","ref":"refs/merge-requests/7/head","duration":300,"queued_duration":40,"finished_at":%q,"runner":{"description":"shared runner"},"pipeline":{"source":"merge_request_event"}},
		  {"id":2,"name":"test-e2e: [2/2]","status":"success","ref":"main","duration":200,"queued_duration":20,"finished_at":%q,"runner":{"description":"portable"},"pipeline":{"source":"push"}}
		]`, f(time.Minute), f(2*time.Minute))
	})
	return httptest.NewServer(mux), &pages
}

func TestPollRecordsJobsSinceAndStopsPaging(t *testing.T) {
	now := time.Now()
	srv, pages := fakeGitLab(t, now)
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "secret", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "my-group", now.Add(-10*time.Minute), s, quiet); err != nil {
		t.Fatal(err)
	}
	if *pages != 2 {
		t.Errorf("read %d pages of jobs, want 2 (the second holds the first job older than the window)", *pages)
	}
	var b strings.Builder
	s.write(&b)
	out := b.String()
	for _, want := range []string{
		`ci_job_duration_seconds_count{project="my-group/my-project",job="test-e2e",part="1/2",runner="shared runner",status="failed",ref="merge_request"} 1`,
		`ci_job_duration_seconds_sum{project="my-group/my-project",job="test-e2e",part="2/2",runner="portable",status="success",ref="default"} 200`,
		`ci_job_queued_seconds_sum{project="my-group/my-project",job="test-e2e",part="1/2",runner="shared runner",status="failed",ref="merge_request"} 40`,
		`ci_job_duration_seconds_bucket{project="my-group/my-project",job="test-e2e",part="2/2",runner="portable",status="success",ref="default",le="180"} 0`,
		`ci_job_duration_seconds_bucket{project="my-group/my-project",job="test-e2e",part="2/2",runner="portable",status="success",ref="default",le="300"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
	if strings.Contains(out, `job="check-go"`) {
		t.Error("a job older than the window was recorded")
	}

	// A second poll over the same window must not count a job twice.
	if err := poll(context.Background(), g, "my-group", now.Add(-10*time.Minute), s, quiet); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	s.write(&b)
	if !strings.Contains(b.String(), `part="1/2",runner="shared runner",status="failed",ref="merge_request"} 1`) {
		t.Errorf("a job seen twice was counted twice:\n%s", b.String())
	}
}

func TestPollReportsARefusedToken(t *testing.T) {
	srv, _ := fakeGitLab(t, time.Now())
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "wrong", client: newClient()}
	err := poll(context.Background(), g, "my-group", time.Now().Add(-time.Hour), newStore(), quiet)
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("poll with a refused token = %v, want an error naming 401", err)
	}
}

func render(s *store) string {
	var b strings.Builder
	s.write(&b)
	return b.String()
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func TestRedirectIsNotFollowedAndTokenStaysHome(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("PRIVATE-TOKEN") != "" {
			leaked.Store(true)
		}
		fmt.Fprint(w, `[]`)
	}))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/steal", http.StatusFound)
	}))
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "secret", client: newClient()}
	err := poll(context.Background(), g, "my-group", time.Now().Add(-time.Hour), newStore(), quiet)
	if err == nil || !strings.Contains(err.Error(), "302") {
		t.Errorf("poll behind a redirect = %v, want an error naming 302", err)
	}
	if leaked.Load() {
		t.Error("the token was sent to the redirect target")
	}
}

func TestNextPageOnAnotherHostIsIgnored(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked.Store(true)
		fmt.Fprint(w, `[]`)
	}))
	defer other.Close()
	var pages atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		w.Header().Set("Link", fmt.Sprintf(`<%s/api/v4/groups/g/projects?page=2>; rel="next"`, other.URL))
		fmt.Fprint(w, `[]`)
	}))
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "secret", client: newClient()}
	if err := poll(context.Background(), g, "g", time.Now(), newStore(), quiet); err != nil {
		t.Fatal(err)
	}
	if leaked.Load() || pages.Load() != 2 {
		t.Errorf("a Link header to another host was followed (leaked=%v, requests=%d)", leaked.Load(), pages.Load())
	}
}

func TestNextPageStaysOnTheConfiguredBase(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.Query().Get("page"))
		if r.URL.Query().Get("page") == "" {
			w.Header().Set("X-Next-Page", "2")
		}
		fmt.Fprint(w, `[{"id":1}]`)
	}))
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	items, _, err := get[project](context.Background(), g, "x?per_page=1", 5, nil)
	if err != nil || len(items) != 2 || strings.Join(got, ",") != ",2" {
		t.Errorf("items=%d pages=%q err=%v, want 2 items from pages 1 and 2", len(items), got, err)
	}
}

func TestPagingStopsOnOldJobDespiteNullFinishedAt(t *testing.T) {
	now := time.Now()
	var pages atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/p","default_branch":"main"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope[]") == "pending" {
			fmt.Fprint(w, `[]`)
			return
		}
		pages.Add(1)
		w.Header().Set("X-Next-Page", "9")
		fmt.Fprintf(w, `[{"id":1,"name":"a","status":"canceled","finished_at":null,"created_at":%q},
		  {"id":2,"name":"b","status":"success","finished_at":%q,"created_at":%q,"duration":5}]`,
			stamp(now.Add(-time.Minute)), stamp(now.Add(-2*time.Hour)), stamp(now.Add(-3*time.Hour)))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	if err := poll(context.Background(), g, "g", now.Add(-time.Hour), newStore(), quiet); err != nil {
		t.Fatal(err)
	}
	if pages.Load() != 1 {
		t.Errorf("read %d pages, want 1: an old job on the page ends paging", pages.Load())
	}
}

func TestPageCapIsCountedAndPagingEnds(t *testing.T) {
	now := time.Now()
	var pages atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/p","default_branch":"main"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope[]") == "pending" {
			fmt.Fprint(w, `[]`)
			return
		}
		pages.Add(1)
		w.Header().Set("X-Next-Page", "2")
		fmt.Fprintf(w, `[{"id":%d,"name":"a","status":"success","finished_at":%q,"duration":5}]`, pages.Load(), stamp(now))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", now.Add(-time.Hour), s, quiet); err != nil {
		t.Fatal(err)
	}
	if pages.Load() != maxJobPages || s.pageCapHits != 1 {
		t.Errorf("pages=%d hits=%v, want %d pages and 1 hit", pages.Load(), s.pageCapHits, maxJobPages)
	}
}

func TestFailedPollKeepsSinceAndWindowIsCapped(t *testing.T) {
	now := time.Now()
	if got := windowStart(now.Add(-48*time.Hour), now); !got.Equal(now.Add(-maxWindow)) {
		t.Errorf("windowStart after a long outage = %v, want now-%s", got, maxWindow)
	}
	since := now.Add(-time.Hour)
	if got := advance(since, now, 2*time.Minute, errors.New("boom")); !got.Equal(since) {
		t.Errorf("a failed poll moved since to %v", got)
	}
	if got := advance(since, now, 2*time.Minute, nil); !got.Equal(now.Add(-2 * time.Minute)) {
		t.Errorf("a good poll set since to %v", got)
	}
	recent := now.Add(-time.Minute)
	if got := windowStart(recent, now); !got.Equal(recent) {
		t.Errorf("windowStart moved a recent since to %v", got)
	}
}

func TestProjectLabelIsPathWithNamespace(t *testing.T) {
	now := time.Now()
	srv, _ := fakeGitLab(t, now)
	defer srv.Close()
	s := newStore()
	g := gitlab{base: srv.URL, token: "secret", client: newClient()}
	if err := poll(context.Background(), g, "my-group", now.Add(-10*time.Minute), s, quiet); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(render(s), `project="my-group/my-project"`) {
		t.Errorf("no series labeled with path_with_namespace:\n%s", render(s))
	}
}

func TestConfigValidation(t *testing.T) {
	dir := t.TempDir()
	var n int
	write := func(content string) string {
		n++
		p := filepath.Join(dir, fmt.Sprintf("token%d", n))
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := map[string]string{"GITLAB_URL": "https://gitlab.example.com", "GITLAB_GROUP": "g", "GITLAB_TOKEN_FILE": write("tok\n")}
	for name, tc := range map[string]struct {
		set  map[string]string
		fail string
	}{
		"valid":              {nil, ""},
		"interval too short": {map[string]string{"INTERVAL": "5s"}, "INTERVAL"},
		"lookback zero":      {map[string]string{"LOOKBACK": "0s"}, "LOOKBACK"},
		"lookback negative":  {map[string]string{"LOOKBACK": "-1m"}, "LOOKBACK"},
		"http refused":       {map[string]string{"GITLAB_URL": "http://gitlab.example.com"}, "ALLOW_INSECURE_HTTP"},
		"http allowed":       {map[string]string{"GITLAB_URL": "http://gitlab.example.com", "ALLOW_INSECURE_HTTP": "true"}, ""},
		"other scheme":       {map[string]string{"GITLAB_URL": "ftp://gitlab.example.com"}, "https"},
		"empty token":        {map[string]string{"GITLAB_TOKEN_FILE": write(" \n")}, "empty"},
		"control char":       {map[string]string{"GITLAB_TOKEN_FILE": write("ab\x00cd")}, "control character"},
		"inner newline":      {map[string]string{"GITLAB_TOKEN_FILE": write("ab\ncd")}, "control character"},
		"bad ttl":            {map[string]string{"SERIES_TTL": "0s"}, "SERIES_TTL"},
	} {
		t.Run(name, func(t *testing.T) {
			for _, k := range []string{"GITLAB_URL", "GITLAB_GROUP", "GITLAB_TOKEN_FILE", "INTERVAL", "LOOKBACK", "ALLOW_INSECURE_HTTP", "SERIES_TTL"} {
				t.Setenv(k, "")
			}
			for k, v := range base {
				t.Setenv(k, v)
			}
			for k, v := range tc.set {
				t.Setenv(k, v)
			}
			_, err := loadConfig()
			switch {
			case tc.fail == "" && err != nil:
				t.Errorf("loadConfig = %v, want success", err)
			case tc.fail != "" && (err == nil || !strings.Contains(err.Error(), tc.fail)):
				t.Errorf("loadConfig = %v, want an error mentioning %q", err, tc.fail)
			}
		})
	}
}

func TestLabelEscapingSurvivesInvalidUTF8(t *testing.T) {
	l := label(key{job: "a\xffb\"c\\d\ne"})
	if want := "job=\"a\uFFFDb\\\"c\\\\d\\ne\""; !strings.Contains(l, want) {
		t.Errorf("label = %s, want it to contain %s", l, want)
	}
	if !utf8.ValidString(l) {
		t.Errorf("label is not valid UTF-8: %q", l)
	}
}

func TestNullDurationsAreSkipped(t *testing.T) {
	s := newStore()
	d := 12.0
	s.add("g/p", job{ID: 1, Name: "a", Status: "success", FinishedAt: stamp(time.Now()), Duration: &d}, "main")
	out := render(s)
	if !strings.Contains(out, `ci_job_duration_seconds_count{project="g/p",job="a"`) {
		t.Errorf("duration missing:\n%s", out)
	}
	if strings.Contains(out, `ci_job_queued_seconds_count{`) {
		t.Errorf("a null queued_duration was recorded as an observation:\n%s", out)
	}
}

func TestTooManyRequestsIsRetriedOnceWhenShort(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	if err := poll(context.Background(), g, "g", time.Now(), newStore(), quiet); err != nil || calls.Load() != 3 {
		t.Errorf("err=%v calls=%d, want success after one retry", err, calls.Load())
	}
}

func TestTooManyRequestsWithLongWaitAbortsThePoll(t *testing.T) {
	var projects atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a"},{"id":2,"path_with_namespace":"g/b"}]`)
	})
	mux.HandleFunc("/api/v4/projects/", func(w http.ResponseWriter, r *http.Request) {
		projects.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	err := poll(context.Background(), g, "g", time.Now(), newStore(), quiet)
	if err == nil || !strings.Contains(err.Error(), "429") || projects.Load() != 1 {
		t.Errorf("err=%v project requests=%d, want a 429 error after the first project", err, projects.Load())
	}
}

func TestForbiddenProjectIsSkippedNotAnError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusForbidden)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil || s.projectsSkipped != 1 {
		t.Errorf("err=%v skipped=%v, want no error and 1 skip", err, s.projectsSkipped)
	}
}

func TestIdleSeriesExpireAndZeroPollExportsZero(t *testing.T) {
	s := newStore()
	d := 1.0
	s.add("g/p", job{ID: 1, Name: "a", Status: "success", FinishedAt: stamp(time.Now()), Duration: &d}, "main")
	s.expire(time.Now().Add(time.Hour), 2*time.Hour)
	if len(s.duration) != 1 {
		t.Error("a series within its TTL was dropped")
	}
	s.expire(time.Now().Add(3*time.Hour), 2*time.Hour)
	if len(s.duration) != 0 {
		t.Error("a series past its TTL was kept")
	}
	if !strings.Contains(render(s), "ci_job_exporter_last_poll_timestamp_seconds 0\n") {
		t.Errorf("a poll that never ran must export 0:\n%s", render(s))
	}
}

func TestLongPartIsTruncated(t *testing.T) {
	s := newStore()
	d := 1.0
	s.add("g/p", job{ID: 1, Name: "a: [" + strings.Repeat("x", 500) + "]", Status: "success", FinishedAt: stamp(time.Now()), Duration: &d}, "main")
	for k := range s.duration {
		if len(k.part) != maxPartLen {
			t.Errorf("part has %d bytes, want %d", len(k.part), maxPartLen)
		}
	}
}

// pendingGitLab serves one project whose pending jobs a test can change or break.
func pendingGitLab(t *testing.T, body *atomic.Value, status *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope[]") != "pending" {
			fmt.Fprint(w, `[]`)
			return
		}
		if c := status.Load(); c != 0 {
			http.Error(w, "no", int(c))
			return
		}
		fmt.Fprint(w, body.Load().(string))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestPendingJobsAreCountedPerProjectAndSortedTags(t *testing.T) {
	var body atomic.Value
	var status atomic.Int32
	now := time.Now().Truncate(time.Second)
	body.Store(fmt.Sprintf(`[
	  {"id":1,"tag_list":["light","heavy"],"created_at":%q},
	  {"id":2,"tag_list":["heavy","light"],"created_at":%q},
	  {"id":3,"tag_list":["heavy"],"created_at":%q},
	  {"id":4,"tag_list":[],"created_at":%q}
	]`, stamp(now.Add(-10*time.Minute)), stamp(now.Add(-90*time.Second)), stamp(now.Add(-30*time.Second)), stamp(now.Add(-5*time.Second))))
	srv := pendingGitLab(t, &body, &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	s.now = func() time.Time { return now }
	if err := poll(context.Background(), g, "g", now, s, quiet); err != nil {
		t.Fatal(err)
	}
	out := render(s)
	for _, want := range []string{
		`ci_jobs_pending{project="g/a",tags="heavy,light"} 2`,
		`ci_jobs_pending{project="g/a",tags="heavy"} 1`,
		`ci_jobs_pending{project="g/a",tags=""} 1`,
		`ci_job_pending_oldest_seconds{project="g/a",tags="heavy,light"} 600`,
		`ci_job_pending_oldest_seconds{project="g/a",tags="heavy"} 30`,
		`ci_job_pending_oldest_seconds{project="g/a",tags=""} 5`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
}

func TestPendingSnapshotIsReplacedAndFailedPollKeepsIt(t *testing.T) {
	var body atomic.Value
	var status atomic.Int32
	now := time.Now()
	body.Store(fmt.Sprintf(`[{"id":1,"tag_list":["heavy"],"created_at":%q}]`, stamp(now)))
	srv := pendingGitLab(t, &body, &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", now, s, quiet); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(render(s), `ci_jobs_pending{project="g/a",tags="heavy"} 1`) {
		t.Fatalf("first snapshot missing:\n%s", render(s))
	}

	status.Store(http.StatusInternalServerError)
	if err := poll(context.Background(), g, "g", now, s, quiet); err == nil {
		t.Fatal("poll with a failing pending read returned no error")
	}
	if !strings.Contains(render(s), `ci_jobs_pending{project="g/a",tags="heavy"} 1`) {
		t.Errorf("a failed poll dropped the last snapshot:\n%s", render(s))
	}

	status.Store(0)
	body.Store(`[]`)
	if err := poll(context.Background(), g, "g", now, s, quiet); err != nil {
		t.Fatal(err)
	}
	if out := render(s); strings.Contains(out, `ci_jobs_pending{`) || strings.Contains(out, `ci_job_pending_oldest_seconds{`) {
		t.Errorf("a project without pending jobs kept its series:\n%s", out)
	}
}

func TestForbiddenPendingReadIsSkippedNotAnError(t *testing.T) {
	var body atomic.Value
	var status atomic.Int32
	body.Store(`[]`)
	status.Store(http.StatusForbidden)
	srv := pendingGitLab(t, &body, &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil || s.projectsSkipped != 1 {
		t.Errorf("err=%v skipped=%v, want no error and 1 skip", err, s.projectsSkipped)
	}
}

func TestPendingTagsAreEscapedAndCapped(t *testing.T) {
	snap := map[pendingKey]*pendingStat{}
	addPending(snap, "g/a", job{TagList: []string{"b\"q", strings.Repeat("x", 500)}})
	s := newStore()
	s.pending = snap
	for k := range snap {
		if utf8.RuneCountInString(k.tags) != maxTagsLen {
			t.Errorf("tags have %d runes, want %d", utf8.RuneCountInString(k.tags), maxTagsLen)
		}
	}
	s2 := newStore()
	s2.pending = map[pendingKey]*pendingStat{{"g/a", "q\"\xff"}: {count: 1}}
	out := render(s2)
	if !strings.Contains(out, `tags="q\"`+"�"+`"`) || !utf8.ValidString(out) {
		t.Errorf("tags label not escaped or cleaned:\n%s", out)
	}
}

func TestPendingPagingIsCapped(t *testing.T) {
	var pages atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/p"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope[]") != "pending" {
			fmt.Fprint(w, `[]`)
			return
		}
		pages.Add(1)
		w.Header().Set("X-Next-Page", "2")
		fmt.Fprint(w, `[{"id":1,"tag_list":["a"]}]`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	if pages.Load() != maxJobPages || s.pageCapHits != 1 {
		t.Errorf("pages=%d hits=%v, want %d pages and 1 hit", pages.Load(), s.pageCapHits, maxJobPages)
	}
	if !strings.Contains(render(s), `ci_jobs_pending{project="g/p",tags="a"} 20`+"\n") {
		t.Errorf("pending jobs of capped pages not counted:\n%s", render(s))
	}
}

// pipelinesGitLab serves two projects whose pipelines per status a test can change or break.
func pipelinesGitLab(t *testing.T, bodies map[string]*atomic.Value, status *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a"},{"id":2,"path_with_namespace":"g/b"}]`)
	})
	for _, id := range []string{"1", "2"} {
		mux.HandleFunc("/api/v4/projects/"+id+"/jobs", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `[]`)
		})
		mux.HandleFunc("/api/v4/projects/"+id+"/pipelines", func(w http.ResponseWriter, r *http.Request) {
			if c := status.Load(); c != 0 {
				http.Error(w, "no", int(c))
				return
			}
			if r.URL.Query().Get("scope") == "finished" {
				fmt.Fprint(w, `[]`)
				return
			}
			b := bodies[r.URL.Query().Get("status")]
			if b == nil {
				http.Error(w, "unexpected status", http.StatusBadRequest)
				return
			}
			if id == "2" {
				fmt.Fprint(w, `[]`)
				return
			}
			fmt.Fprint(w, b.Load().(string))
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newPipelineBodies(pending, running string) map[string]*atomic.Value {
	m := map[string]*atomic.Value{"pending": {}, "running": {}}
	m["pending"].Store(pending)
	m["running"].Store(running)
	return m
}

func TestPipelinesAreCountedPerProjectAndStatus(t *testing.T) {
	var status atomic.Int32
	srv := pipelinesGitLab(t, newPipelineBodies(`[{"id":1},{"id":2},{"id":3}]`, `[{"id":4}]`), &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	out := render(s)
	for _, want := range []string{`ci_pipelines_pending{project="g/a"} 3`, `ci_pipelines_running{project="g/a"} 1`} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
	if strings.Contains(out, `project="g/b"} `) {
		t.Errorf("a project without pipelines has a series:\n%s", out)
	}
}

func TestPipelineSnapshotIsReplacedAndFailedPollKeepsIt(t *testing.T) {
	var status atomic.Int32
	bodies := newPipelineBodies(`[{"id":1}]`, `[{"id":2},{"id":3}]`)
	srv := pipelinesGitLab(t, bodies, &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}

	status.Store(http.StatusInternalServerError)
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err == nil {
		t.Fatal("poll with a failing pipelines read returned no error")
	}
	out := render(s)
	if !strings.Contains(out, `ci_pipelines_pending{project="g/a"} 1`) || !strings.Contains(out, `ci_pipelines_running{project="g/a"} 2`) {
		t.Errorf("a failed poll dropped the last snapshot:\n%s", out)
	}

	status.Store(0)
	bodies["pending"].Store(`[]`)
	bodies["running"].Store(`[]`)
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	if out := render(s); strings.Contains(out, `ci_pipelines_pending{`) || strings.Contains(out, `ci_pipelines_running{`) {
		t.Errorf("a project without pipelines kept its series:\n%s", out)
	}
}

func TestForbiddenPipelinesReadIsSkippedNotAnError(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusForbidden)
	srv := pipelinesGitLab(t, newPipelineBodies(`[]`, `[]`), &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil || s.projectsSkipped != 2 {
		t.Errorf("err=%v skipped=%v, want no error and 2 skips", err, s.projectsSkipped)
	}
}

// finishedGitLab serves one project with a default branch, its finished pipelines and each one's details.
func finishedGitLab(t *testing.T, list, latest *atomic.Value, details map[string]string, calls *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a","default_branch":"main"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
	mux.HandleFunc("/api/v4/projects/1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case q.Get("scope") != "finished":
			fmt.Fprint(w, `[]`)
		case q.Get("ref") == "main":
			fmt.Fprint(w, latest.Load().(string))
		case q.Get("updated_after") == "":
			http.Error(w, "finished pipelines read without a window", http.StatusBadRequest)
		default:
			fmt.Fprint(w, list.Load().(string))
		}
	})
	mux.HandleFunc("/api/v4/projects/1/pipelines/{id}", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		d, ok := details[r.PathValue("id")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, d)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestFinishedPipelinesAreTimedOncePerFinish(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var list, latest atomic.Value
	var calls atomic.Int32
	list.Store(`[{"id":7},{"id":8},{"id":9}]`)
	latest.Store(`[{"id":9,"status":"failed"}]`)
	details := map[string]string{
		"7": fmt.Sprintf(`{"id":7,"status":"success","ref":"main","source":"push","created_at":%q,"finished_at":%q,"queued_duration":40}`, stamp(now.Add(-20*time.Minute)), stamp(now.Add(-5*time.Minute))),
		"8": fmt.Sprintf(`{"id":8,"status":"failed","ref":"refs/merge-requests/3/head","source":"merge_request_event","created_at":%q,"finished_at":%q}`, stamp(now.Add(-3*time.Minute)), stamp(now.Add(-time.Minute))),
		"9": fmt.Sprintf(`{"id":9,"status":"success","ref":"v1.0.0","tag":true,"source":"push","created_at":%q,"finished_at":%q,"queued_duration":5}`, stamp(now.Add(-2*time.Hour)), stamp(now.Add(-90*time.Minute))),
	}
	srv := finishedGitLab(t, &list, &latest, details, &calls)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	since := now.Add(-time.Hour)
	if err := poll(context.Background(), g, "g", since, s, quiet); err != nil {
		t.Fatal(err)
	}
	out := render(s)
	for _, want := range []string{
		`ci_pipeline_duration_seconds_bucket{project="g/a",status="success",ref="default",le="900"} 1`,
		`ci_pipeline_duration_seconds_bucket{project="g/a",status="success",ref="default",le="600"} 0`,
		`ci_pipeline_duration_seconds_sum{project="g/a",status="success",ref="default"} 900`,
		`ci_pipeline_queued_seconds_sum{project="g/a",status="success",ref="default"} 40`,
		`ci_pipeline_duration_seconds_count{project="g/a",status="failed",ref="merge_request"} 1`,
		`ci_default_branch_failed{project="g/a"} 1`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
	if strings.Contains(out, `ci_pipeline_queued_seconds_count{project="g/a",status="failed",ref="merge_request"}`) {
		t.Errorf("a pipeline without a queued time has a queued series:\n%s", out)
	}
	if strings.Contains(out, `ref="tag"`) {
		t.Errorf("a pipeline that finished before the window was counted:\n%s", out)
	}

	if err := poll(context.Background(), g, "g", since, s, quiet); err != nil {
		t.Fatal(err)
	}
	if out := render(s); !strings.Contains(out, `ci_pipeline_duration_seconds_count{project="g/a",status="success",ref="default"} 1`+"\n") {
		t.Errorf("a pipeline seen twice was counted twice:\n%s", out)
	}

	details["7"] = fmt.Sprintf(`{"id":7,"status":"failed","ref":"main","source":"push","created_at":%q,"finished_at":%q}`, stamp(now.Add(-20*time.Minute)), stamp(now))
	latest.Store(`[{"id":7,"status":"success"}]`)
	if err := poll(context.Background(), g, "g", since, s, quiet); err != nil {
		t.Fatal(err)
	}
	out = render(s)
	if !strings.Contains(out, `ci_pipeline_duration_seconds_count{project="g/a",status="failed",ref="default"} 1`+"\n") {
		t.Errorf("a retried pipeline that finished again was not counted:\n%s", out)
	}
	if !strings.Contains(out, `ci_default_branch_failed{project="g/a"} 0`+"\n") {
		t.Errorf("the default branch stayed failed after a green pipeline:\n%s", out)
	}
}

func TestMissingPipelineDetailIsSkippedNotAnError(t *testing.T) {
	var list, latest atomic.Value
	var calls atomic.Int32
	list.Store(`[{"id":404}]`)
	latest.Store(`[]`)
	srv := finishedGitLab(t, &list, &latest, map[string]string{}, &calls)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now().Add(-time.Hour), s, quiet); err != nil {
		t.Fatalf("a pipeline deleted between list and read failed the poll: %v", err)
	}
	if calls.Load() != 1 || strings.Contains(render(s), "ci_default_branch_failed{") {
		t.Errorf("calls=%d, metrics:\n%s", calls.Load(), render(s))
	}
}

// openCountsGitLab serves three projects: one with both counts, one with issues disabled
// and one whose merge request total GitLab leaves out. A test can change the counts or break the reads.
func openCountsGitLab(t *testing.T, issues, mrs *atomic.Int32, status *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `[{"id":1,"path_with_namespace":"g/b","open_issues_count":%d},{"id":2,"path_with_namespace":"g/a"},{"id":3,"path_with_namespace":"g/huge","open_issues_count":0}]`, issues.Load())
	})
	for _, id := range []string{"1", "2", "3"} {
		mux.HandleFunc("/api/v4/projects/"+id+"/jobs", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
		mux.HandleFunc("/api/v4/projects/"+id+"/pipelines", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
		mux.HandleFunc("/api/v4/projects/"+id+"/merge_requests", func(w http.ResponseWriter, r *http.Request) {
			if c := status.Load(); c != 0 {
				http.Error(w, "no", int(c))
				return
			}
			q := r.URL.Query()
			if q.Get("state") == "merged" {
				fmt.Fprint(w, `[]`)
				return
			}
			if q.Get("state") != "opened" || q.Get("per_page") != "100" {
				http.Error(w, "unexpected query", http.StatusBadRequest)
				return
			}
			if id == "3" {
				w.Header().Set("X-Next-Page", "2")
				fmt.Fprint(w, `[{"id":1}]`)
				return
			}
			w.Header().Set("X-Total", fmt.Sprint(mrs.Load()))
			fmt.Fprint(w, `[]`)
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestOpenCountsAreExportedPerProjectAndSorted(t *testing.T) {
	var issues, mrs, status atomic.Int32
	issues.Store(5)
	mrs.Store(3)
	srv := openCountsGitLab(t, &issues, &mrs, &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	out := render(s)
	for _, want := range []string{
		"# TYPE ci_project_open_issues gauge\n",
		"# TYPE ci_project_open_merge_requests gauge\n",
		`ci_project_open_issues{project="g/b"} 5` + "\n" + `ci_project_open_issues{project="g/huge"} 0` + "\n",
		`ci_project_open_merge_requests{project="g/a"} 3` + "\n" + `ci_project_open_merge_requests{project="g/b"} 3` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
	if strings.Contains(out, `ci_project_open_issues{project="g/a"}`) {
		t.Errorf("a project with issues disabled has an issue series:\n%s", out)
	}
	if strings.Contains(out, `ci_project_open_merge_requests{project="g/huge"}`) {
		t.Errorf("a project without an X-Total header has a merge request series:\n%s", out)
	}
}

func TestOpenCountsAreReplacedAndFailedPollKeepsThem(t *testing.T) {
	var issues, mrs, status atomic.Int32
	issues.Store(5)
	mrs.Store(3)
	srv := openCountsGitLab(t, &issues, &mrs, &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}

	issues.Store(7)
	mrs.Store(0)
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	out := render(s)
	if !strings.Contains(out, `ci_project_open_issues{project="g/b"} 7`) || !strings.Contains(out, `ci_project_open_merge_requests{project="g/b"} 0`) {
		t.Errorf("a second poll did not replace the counts:\n%s", out)
	}

	issues.Store(9)
	mrs.Store(4)
	status.Store(http.StatusInternalServerError)
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err == nil {
		t.Fatal("poll with a failing merge request read returned no error")
	}
	out = render(s)
	if !strings.Contains(out, `ci_project_open_issues{project="g/b"} 7`) || !strings.Contains(out, `ci_project_open_merge_requests{project="g/b"} 0`) {
		t.Errorf("a failed poll dropped the last counts:\n%s", out)
	}
}

func TestForbiddenMergeRequestReadIsSkippedNotAnError(t *testing.T) {
	var issues, mrs, status atomic.Int32
	status.Store(http.StatusForbidden)
	srv := openCountsGitLab(t, &issues, &mrs, &status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil || s.projectsSkipped != 3 {
		t.Errorf("err=%v skipped=%v, want no error and 3 skips", err, s.projectsSkipped)
	}
}

func TestAProjectWhoseJobsAreRefusedStillHasItsIssueCount(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/no-ci","open_issues_count":4}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusForbidden) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	s := newStore()
	if err := poll(context.Background(), gitlab{base: srv.URL, token: "t", client: newClient()}, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	if out := render(s); !strings.Contains(out, `ci_project_open_issues{project="g/no-ci"} 4`) {
		t.Errorf("the issue count of a project without CI is missing:\n%s", out)
	}
}

// flowGitLab serves one project whose open and merged merge request lists a test can change or break.
func flowGitLab(t *testing.T, open, merged *atomic.Value, status *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
	mux.HandleFunc("/api/v4/projects/1/pipelines", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
	mux.HandleFunc("/api/v4/projects/1/merge_requests", func(w http.ResponseWriter, r *http.Request) {
		if c := status.Load(); c != 0 {
			http.Error(w, "no", int(c))
			return
		}
		switch r.URL.Query().Get("state") {
		case "opened":
			fmt.Fprint(w, open.Load().(string))
		case "merged":
			if r.URL.Query().Get("updated_after") == "" {
				http.Error(w, "no window", http.StatusBadRequest)
				return
			}
			fmt.Fprint(w, merged.Load().(string))
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newFlow(open, merged string) (*atomic.Value, *atomic.Value, *atomic.Int32) {
	var o, m atomic.Value
	var st atomic.Int32
	o.Store(open)
	m.Store(merged)
	return &o, &m, &st
}

func TestMergeRequestFlowIsExported(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	open, merged, status := newFlow(fmt.Sprintf(`[
	  {"id":1,"created_at":%q,"draft":true,"detailed_merge_status":"draft_status"},
	  {"id":2,"created_at":%q,"detailed_merge_status":"ci_must_pass"},
	  {"id":3,"created_at":%q,"detailed_merge_status":"mergeable"},
	  {"id":4,"created_at":%q,"detailed_merge_status":"mergeable"},
	  {"id":5,"created_at":%q,"detailed_merge_status":"%s"}]`,
		stamp(now.Add(-100*time.Hour)), stamp(now.Add(-2*time.Hour)), stamp(now.Add(-time.Hour)), stamp(now.Add(-time.Minute)), stamp(now), strings.Repeat("x", 40)),
		fmt.Sprintf(`[{"id":10,"created_at":%q,"merged_at":%q},{"id":11,"created_at":%q,"merged_at":%q},{"id":12,"created_at":%q,"merged_at":%q}]`,
			stamp(now.Add(-3*time.Hour)), stamp(now.Add(-time.Hour)),
			stamp(now.Add(-30*time.Hour)), stamp(now.Add(-5*time.Minute)),
			stamp(now.Add(-50*time.Hour)), stamp(now.Add(-20*time.Minute))))
	srv := flowGitLab(t, open, merged, status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	s.now = func() time.Time { return now }
	// Merge request 12 was merged before the window, so a poll that overlaps an earlier one must not count it.
	since := now.Add(-10 * time.Minute)
	if err := poll(context.Background(), g, "g", since, s, quiet); err != nil {
		t.Fatal(err)
	}
	out := render(s)
	for _, want := range []string{
		`ci_project_open_merge_requests{project="g/a"} 5` + "\n",
		`ci_merge_request_oldest_open_seconds{project="g/a"} 7200` + "\n",
		`ci_merge_requests_open_by_state{project="g/a",state="ci_must_pass"} 1` + "\n",
		`ci_merge_requests_open_by_state{project="g/a",state="draft_status"} 1` + "\n",
		`ci_merge_requests_open_by_state{project="g/a",state="mergeable"} 2` + "\n",
		`ci_merge_requests_open_by_state{project="g/a",state="` + strings.Repeat("x", 32) + `"} 1` + "\n",
		`ci_merge_request_time_to_merge_seconds_count{project="g/a"} 1` + "\n",
		`ci_merge_request_time_to_merge_seconds_sum{project="g/a"} 107700` + "\n",
		`ci_merge_request_time_to_merge_seconds_bucket{project="g/a",le="86400"} 0` + "\n",
		`ci_merge_request_time_to_merge_seconds_bucket{project="g/a",le="172800"} 1` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
}

func TestMergedMergeRequestIsObservedOncePerId(t *testing.T) {
	now := time.Now()
	open, merged, status := newFlow(`[]`, fmt.Sprintf(`[{"id":10,"created_at":%q,"merged_at":%q}]`, stamp(now.Add(-2*time.Hour)), stamp(now.Add(-time.Minute))))
	srv := flowGitLab(t, open, merged, status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	for range 2 {
		if err := poll(context.Background(), g, "g", now.Add(-10*time.Minute), s, quiet); err != nil {
			t.Fatal(err)
		}
	}
	if out := render(s); !strings.Contains(out, `ci_merge_request_time_to_merge_seconds_count{project="g/a"} 1`+"\n") {
		t.Errorf("a merge request seen in two polls was not counted once:\n%s", out)
	}
	if out := render(s); strings.Contains(out, "ci_merge_request_oldest_open_seconds{") {
		t.Errorf("a project without open merge requests has an age series:\n%s", out)
	}
}

func TestMergeRequestFlowGaugesSurviveAFailedPollAndAreReplaced(t *testing.T) {
	now := time.Now()
	open, merged, status := newFlow(fmt.Sprintf(`[{"id":2,"created_at":%q,"detailed_merge_status":"mergeable"}]`, stamp(now.Add(-time.Hour))), `[]`)
	srv := flowGitLab(t, open, merged, status)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	s.now = func() time.Time { return now }
	if err := poll(context.Background(), g, "g", now, s, quiet); err != nil {
		t.Fatal(err)
	}
	open.Store(fmt.Sprintf(`[{"id":2,"created_at":%q,"detailed_merge_status":"conflict"}]`, stamp(now.Add(-time.Hour))))
	status.Store(http.StatusInternalServerError)
	if err := poll(context.Background(), g, "g", now, s, quiet); err == nil {
		t.Fatal("a failing read returned no error")
	}
	if out := render(s); !strings.Contains(out, `state="mergeable"} 1`) || strings.Contains(out, `state="conflict"`) {
		t.Errorf("a failed poll changed the state counts:\n%s", out)
	}
	status.Store(0)
	if err := poll(context.Background(), g, "g", now, s, quiet); err != nil {
		t.Fatal(err)
	}
	if out := render(s); strings.Contains(out, `state="mergeable"`) || !strings.Contains(out, `state="conflict"} 1`) {
		t.Errorf("a good poll did not replace the state counts:\n%s", out)
	}
}

func TestOpenMergeRequestsAtThePageLimitHaveNoStateCounts(t *testing.T) {
	var issues, mrs, status atomic.Int32
	srv := openCountsGitLab(t, &issues, &mrs, &status)
	s := newStore()
	if err := poll(context.Background(), gitlab{base: srv.URL, token: "t", client: newClient()}, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	if out := render(s); strings.Contains(out, `ci_merge_requests_open_by_state{project="g/huge"`) || s.pageCapHits == 0 {
		t.Errorf("a capped list has state counts or the cap went uncounted (%v):\n%s", s.pageCapHits, out)
	}
}

// retryGitLab serves one project whose finished jobs a test can swap between polls.
func retryGitLab(t *testing.T, jobs *atomic.Value) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a","default_branch":"main"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope[]") == "pending" {
			fmt.Fprint(w, `[]`)
			return
		}
		if r.URL.Query().Get("include_retried") != "true" {
			http.Error(w, "retried jobs not asked for", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, jobs.Load().(string))
	})
	mux.HandleFunc("/api/v4/projects/1/pipelines", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func retryJob(id int, name, status string, pipeline int, retried bool, at time.Time) string {
	return fmt.Sprintf(`{"id":%d,"name":%q,"status":%q,"ref":"feature","duration":60,"finished_at":%q,"retried":%t,"pipeline":{"id":%d}}`, id, name, status, stamp(at), retried, pipeline)
}

func TestRetriedJobsAreCountedOnceAndKeepTheHistograms(t *testing.T) {
	now := time.Now()
	var jobs atomic.Value
	jobs.Store("[" + strings.Join([]string{
		retryJob(3, "test: [1/2]", "success", 7, false, now.Add(-time.Minute)),
		retryJob(2, "test: [1/2]", "failed", 7, false, now.Add(-2*time.Minute)),
		retryJob(1, "lint", "failed", 7, true, now.Add(-3*time.Minute)),
	}, ",") + "]")
	srv := retryGitLab(t, &jobs)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	for range 2 {
		if err := poll(context.Background(), g, "g", now.Add(-time.Hour), s, quiet); err != nil {
			t.Fatal(err)
		}
	}
	out := render(s)
	for _, want := range []string{
		`ci_job_retried_total{project="g/a",job="test",ref="branch"} 1` + "\n",
		`ci_job_retried_total{project="g/a",job="lint",ref="branch"} 1` + "\n",
		`ci_job_retry_passed_total{project="g/a",job="test",ref="branch"} 1` + "\n",
		`ci_job_duration_seconds_count{project="g/a",job="test",part="1/2",runner="none",status="success",ref="branch"} 1` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
	if strings.Contains(out, `job="lint",part`) {
		t.Errorf("a job marked retried is in the duration histograms:\n%s", out)
	}
	if strings.Contains(out, `ci_job_retry_passed_total{project="g/a",job="lint"`) {
		t.Errorf("a job without a passing retry counts as one that passed:\n%s", out)
	}
}

func TestARetryInALaterPollStillCountsTheEarlierRun(t *testing.T) {
	now := time.Now()
	var jobs atomic.Value
	jobs.Store("[" + retryJob(1, "build", "failed", 9, false, now.Add(-5*time.Minute)) + "]")
	srv := retryGitLab(t, &jobs)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", now.Add(-time.Hour), s, quiet); err != nil {
		t.Fatal(err)
	}
	if out := render(s); strings.Contains(out, "ci_job_retried_total{") {
		t.Fatalf("a job nobody retried is counted as retried:\n%s", out)
	}
	jobs.Store("[" + retryJob(2, "build", "failed", 9, false, now.Add(-time.Minute)) + "," + retryJob(3, "build", "success", 9, false, now) + "]")
	if err := poll(context.Background(), g, "g", now.Add(-time.Hour), s, quiet); err != nil {
		t.Fatal(err)
	}
	out := render(s)
	for _, want := range []string{`ci_job_retried_total{project="g/a",job="build",ref="branch"} 2` + "\n", `ci_job_retry_passed_total{project="g/a",job="build",ref="branch"} 1` + "\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
}

func TestIdleRetryCountersExpire(t *testing.T) {
	s := newStore()
	now := time.Now()
	bump(s.retried, retryKey{"g/a", "j", "branch"}, now.Add(-3*time.Hour))
	bump(s.testFailures, testKey{"g/a", "s", "t"}, now.Add(-3*time.Hour))
	s.expire(now, time.Hour)
	if len(s.retried) != 0 || len(s.testFailures) != 0 {
		t.Errorf("series idle for longer than the ttl are kept: %v %v", s.retried, s.testFailures)
	}
}

// reportGitLab serves one project with the failed pipelines in details and a test report for each of them.
func reportGitLab(t *testing.T, now time.Time, summaryStatus int, reportReads *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1/jobs", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
	mux.HandleFunc("/api/v4/projects/1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("scope") == "finished" && r.URL.Query().Get("updated_after") != "" {
			fmt.Fprint(w, `[{"id":10},{"id":11},{"id":12}]`)
			return
		}
		fmt.Fprint(w, `[]`)
	})
	mux.HandleFunc("/api/v4/projects/1/pipelines/{id}", func(w http.ResponseWriter, r *http.Request) {
		status := "failed"
		if r.PathValue("id") == "12" {
			status = "success"
		}
		fmt.Fprintf(w, `{"id":%s,"status":%q,"ref":"x","created_at":%q,"finished_at":%q}`, r.PathValue("id"), status, stamp(now.Add(-time.Minute)), stamp(now))
	})
	mux.HandleFunc("/api/v4/projects/1/merge_requests", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
	mux.HandleFunc("/api/v4/projects/1/pipelines/{id}/test_report_summary", func(w http.ResponseWriter, r *http.Request) {
		if summaryStatus != 0 {
			http.Error(w, "no", summaryStatus)
			return
		}
		count := 3
		if r.PathValue("id") == "11" {
			count = 0
		}
		fmt.Fprintf(w, `{"total":{"count":%d}}`, count)
	})
	mux.HandleFunc("/api/v4/projects/1/pipelines/{id}/test_report", func(w http.ResponseWriter, r *http.Request) {
		reportReads.Add(1)
		cases := []string{
			`{"status":"failed","name":"TestA","classname":"pkg.Suite"}`,
			`{"status":"success","name":"TestOk","classname":"pkg.Suite"}`,
			`{"status":"error","name":"TestB"}`,
		}
		for i := range 60 {
			cases = append(cases, fmt.Sprintf(`{"status":"failed","name":"Many%d","classname":"pkg.Many"}`, i))
		}
		fmt.Fprintf(w, `{"test_suites":[{"name":%q,"test_cases":[%s]}]}`, strings.Repeat("s", 200), strings.Join(cases, ","))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestFailingTestsAreCountedOncePerFailedPipeline(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var reads atomic.Int32
	srv := reportGitLab(t, now, 0, &reads)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	for range 2 {
		if err := poll(context.Background(), g, "g", now.Add(-time.Hour), s, quiet); err != nil {
			t.Fatal(err)
		}
	}
	if reads.Load() != 1 {
		t.Errorf("read %d test reports, want 1: one failed pipeline has tests, one has none, one passed, and none is read twice", reads.Load())
	}
	out := render(s)
	suite := strings.Repeat("s", maxLabelLen)
	for _, want := range []string{
		`ci_test_failures_total{project="g/a",suite="` + suite + `",test="pkg.Suite.TestA"} 1` + "\n",
		`ci_test_failures_total{project="g/a",suite="` + suite + `",test="TestB"} 1` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
	if n := strings.Count(out, "\nci_test_failures_total{"); n != maxFailedCases {
		t.Errorf("%d test series from one pipeline, want the cap of %d", n, maxFailedCases)
	}
	if strings.Contains(out, "TestOk") {
		t.Errorf("a passing test case is counted as a failure:\n%s", out)
	}
}

func TestForbiddenTestReportIsSkippedNotAnError(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var reads atomic.Int32
	srv := reportGitLab(t, now, http.StatusForbidden, &reads)
	s := newStore()
	if err := poll(context.Background(), gitlab{base: srv.URL, token: "t", client: newClient()}, "g", now.Add(-time.Hour), s, quiet); err != nil {
		t.Fatalf("a refused test report ended the poll: %v", err)
	}
	if s.projectsSkipped != 1 || reads.Load() != 0 || strings.Contains(render(s), "\nci_test_failures_total{") {
		t.Errorf("skipped=%v reads=%d, want one skip (the project is not asked again) and no test series", s.projectsSkipped, reads.Load())
	}
}

func TestTestSeriesAreCapped(t *testing.T) {
	s := newStore()
	for i := range maxTestSeries/maxFailedCases + 5 {
		var cases []string
		for j := range maxFailedCases {
			cases = append(cases, fmt.Sprintf(`{"status":"failed","name":"t%d-%d"}`, i, j))
		}
		var r testReport
		if err := json.Unmarshal([]byte(`{"test_suites":[{"name":"s","test_cases":[`+strings.Join(cases, ",")+`]}]}`), &r); err != nil {
			t.Fatal(err)
		}
		s.addTestFailures("g/a", r)
	}
	if len(s.testFailures) != maxTestSeries {
		t.Errorf("%d test series, want the cap of %d", len(s.testFailures), maxTestSeries)
	}
}

// groupGitLab serves two projects, one with storage statistics and one without, and the group's
// milestones. A test can change the counts or make the milestone, statistics or issue reads fail.
func groupGitLab(t *testing.T, opened *atomic.Int32, milestoneStatus, statsStatus, issueStatus *atomic.Int32, pages *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/groups/g/projects", func(w http.ResponseWriter, r *http.Request) {
		// As GitLab 19 does: the list carries statistics: null, only the project itself has the sizes.
		fmt.Fprint(w, `[{"id":1,"path_with_namespace":"g/a","open_issues_count":4,"statistics":null},
		  {"id":2,"path_with_namespace":"g/b","open_issues_count":1,"statistics":null},{"id":3,"path_with_namespace":"g/noissues"}]`)
	})
	mux.HandleFunc("/api/v4/projects/1", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("statistics") != "true" {
			http.Error(w, "statistics not asked for", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, `{"id":1,"path_with_namespace":"g/a","statistics":{"repository_size":1000,"job_artifacts_size":2500.5,"container_registry_size":7,"commit_count":9}}`)
	})
	mux.HandleFunc("/api/v4/projects/2", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusForbidden) })
	mux.HandleFunc("/api/v4/projects/3", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":3,"path_with_namespace":"g/noissues"}`)
	})
	for _, id := range []string{"1", "2", "3"} {
		mux.HandleFunc("/api/v4/projects/"+id+"/jobs", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
		mux.HandleFunc("/api/v4/projects/"+id+"/pipelines", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
		mux.HandleFunc("/api/v4/projects/"+id+"/merge_requests", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `[]`) })
		mux.HandleFunc("/api/v4/projects/"+id+"/issues", func(w http.ResponseWriter, r *http.Request) {
			if c := issueStatus.Load(); c != 0 {
				http.Error(w, "no", int(c))
				return
			}
			if q := r.URL.Query(); q.Get("state") != "opened" || q.Get("assignee_id") != "None" || q.Get("per_page") != "1" {
				http.Error(w, "unexpected query", http.StatusBadRequest)
				return
			}
			if id == "1" {
				w.Header().Set("X-Total", "3")
			}
			fmt.Fprint(w, `[]`)
		})
	}
	mux.HandleFunc("/api/v4/groups/g/milestones", func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		if c := milestoneStatus.Load(); c != 0 {
			http.Error(w, "no", int(c))
			return
		}
		if q := r.URL.Query(); q.Get("state") != "active" || q.Get("per_page") != "100" {
			http.Error(w, "unexpected query", http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Next-Page", "2")
		fmt.Fprintf(w, `[{"title":"v1.0","due_date":"2026-12-01"},{"title":"later"},{"title":"v1.0","due_date":"2027-01-01"},{"title":%q}]`, strings.Repeat("m", 200))
	})
	mux.HandleFunc("/api/v4/groups/g/issues_statistics", func(w http.ResponseWriter, r *http.Request) {
		if c := statsStatus.Load(); c != 0 {
			http.Error(w, "no", int(c))
			return
		}
		n := int(opened.Load())
		if r.URL.Query().Get("milestone") == "later" {
			n = 0
		}
		fmt.Fprintf(w, `{"statistics":{"counts":{"all":%d,"closed":2,"opened":%d}}}`, n+2, n)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestMilestonesUnassignedAndStorageAreExported(t *testing.T) {
	var opened, ms, st, is, pages atomic.Int32
	opened.Store(5)
	srv := groupGitLab(t, &opened, &ms, &st, &is, &pages)
	s := newStore()
	if err := poll(context.Background(), gitlab{base: srv.URL, token: "t", client: newClient()}, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	out := render(s)
	for _, want := range []string{
		`ci_milestone_issues{milestone="later",state="opened"} 0` + "\n" + `ci_milestone_issues{milestone="later",state="closed"} 2` + "\n",
		`ci_milestone_issues{milestone="v1.0",state="opened"} 5` + "\n" + `ci_milestone_issues{milestone="v1.0",state="closed"} 2` + "\n",
		`ci_milestone_due_timestamp_seconds{milestone="v1.0"} 1796083200` + "\n",
		`ci_milestone_issues{milestone="` + strings.Repeat("m", maxLabelLen) + `",state="opened"} 5`,
		`ci_issues_unassigned{project="g/a"} 3` + "\n",
		`ci_project_storage_bytes{project="g/a",kind="repository"} 1000` + "\n",
		`ci_project_storage_bytes{project="g/a",kind="job_artifacts"} 2500.5` + "\n",
		`ci_project_storage_bytes{project="g/a",kind="container_registry"} 7` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
	if n := strings.Count(out, `ci_milestone_issues{milestone="v1.0",state="opened"}`); n != 1 {
		t.Errorf("a milestone title that occurs twice has %d series", n)
	}
	for _, bad := range []string{`ci_milestone_due_timestamp_seconds{milestone="later"}`, `ci_project_storage_bytes{project="g/b"`, `ci_issues_unassigned{project="g/b"}`, `ci_issues_unassigned{project="g/noissues"}`, `kind="commit_count"`} {
		if strings.Contains(out, bad) {
			t.Errorf("metrics have a series that should not exist: %s\n%s", bad, out)
		}
	}
	if pages.Load() != 1 {
		t.Errorf("read %d pages of milestones, want only the first", pages.Load())
	}
}

func TestMilestoneAndUnassignedGaugesAreReplacedAndFailedPollKeepsThem(t *testing.T) {
	var opened, ms, st, is, pages atomic.Int32
	opened.Store(5)
	srv := groupGitLab(t, &opened, &ms, &st, &is, &pages)
	g := gitlab{base: srv.URL, token: "t", client: newClient()}
	s := newStore()
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	opened.Store(8)
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err != nil {
		t.Fatal(err)
	}
	if out := render(s); !strings.Contains(out, `ci_milestone_issues{milestone="v1.0",state="opened"} 8`) {
		t.Fatalf("a good poll did not replace the milestone counts:\n%s", out)
	}
	opened.Store(9)
	st.Store(http.StatusInternalServerError)
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err == nil {
		t.Fatal("a failing milestone read returned no error")
	}
	st.Store(0)
	is.Store(http.StatusBadGateway)
	if err := poll(context.Background(), g, "g", time.Now(), s, quiet); err == nil {
		t.Fatal("a failing issue read returned no error")
	}
	out := render(s)
	if !strings.Contains(out, `ci_milestone_issues{milestone="v1.0",state="opened"} 8`) || !strings.Contains(out, `ci_issues_unassigned{project="g/a"} 3`) {
		t.Errorf("a failed poll changed the gauges:\n%s", out)
	}
}

func TestForbiddenMilestonesAndIssuesAreSkippedNotAnError(t *testing.T) {
	var opened, ms, st, is, pages atomic.Int32
	ms.Store(http.StatusForbidden)
	is.Store(http.StatusForbidden)
	srv := groupGitLab(t, &opened, &ms, &st, &is, &pages)
	s := newStore()
	if err := poll(context.Background(), gitlab{base: srv.URL, token: "t", client: newClient()}, "g", time.Now(), s, quiet); err != nil {
		t.Fatalf("refused reads ended the poll: %v", err)
	}
	out := render(s)
	if strings.Contains(out, "\nci_milestone_issues{") || strings.Contains(out, "\nci_issues_unassigned{") || s.projectsSkipped != 2 {
		t.Errorf("skipped=%v, want 2 (the projects with issues enabled) and no series:\n%s", s.projectsSkipped, out)
	}
	if !strings.Contains(out, `ci_project_storage_bytes{project="g/a",kind="repository"} 1000`) {
		t.Errorf("storage depends on the refused reads:\n%s", out)
	}
}
