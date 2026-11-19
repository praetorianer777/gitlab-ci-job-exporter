package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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
		fmt.Fprint(w, `[{"id":69,"path":"my-project","default_branch":"main"}]`)
	})
	mux.HandleFunc("/api/v4/projects/69/jobs", func(w http.ResponseWriter, r *http.Request) {
		pages++
		f := func(d time.Duration) string { return now.Add(-d).UTC().Format(time.RFC3339) }
		if r.URL.Query().Get("page") == "2" {
			fmt.Fprintf(w, `[{"id":3,"name":"check-go","status":"success","ref":"main","duration":100,"queued_duration":1,"finished_at":%q}]`, f(3*time.Hour))
			return
		}
		w.Header().Set("Link", fmt.Sprintf(`<%s/api/v4/projects/69/jobs?page=2>; rel="next"`, "http://"+r.Host))
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
	g := gitlab{base: srv.URL, token: "secret", client: srv.Client()}
	s := newStore()
	if err := poll(context.Background(), g, "my-group", now.Add(-10*time.Minute), s); err != nil {
		t.Fatal(err)
	}
	if *pages != 2 {
		t.Errorf("read %d pages of jobs, want 2 (the second holds the first job older than the window)", *pages)
	}
	var b strings.Builder
	s.write(&b)
	out := b.String()
	for _, want := range []string{
		`ci_job_duration_seconds_count{project="my-project",job="test-e2e",part="1/2",runner="shared runner",status="failed",ref="merge_request"} 1`,
		`ci_job_duration_seconds_sum{project="my-project",job="test-e2e",part="2/2",runner="portable",status="success",ref="default"} 200`,
		`ci_job_queued_seconds_sum{project="my-project",job="test-e2e",part="1/2",runner="shared runner",status="failed",ref="merge_request"} 40`,
		`ci_job_duration_seconds_bucket{project="my-project",job="test-e2e",part="2/2",runner="portable",status="success",ref="default",le="180"} 0`,
		`ci_job_duration_seconds_bucket{project="my-project",job="test-e2e",part="2/2",runner="portable",status="success",ref="default",le="300"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics lack\n  %s\ngot:\n%s", want, out)
		}
	}
	if strings.Contains(out, `job="check-go"`) {
		t.Error("a job older than the window was recorded")
	}

	// A second poll over the same window must not count a job twice.
	if err := poll(context.Background(), g, "my-group", now.Add(-10*time.Minute), s); err != nil {
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
	g := gitlab{base: srv.URL, token: "wrong", client: srv.Client()}
	err := poll(context.Background(), g, "my-group", time.Now().Add(-time.Hour), newStore())
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("poll with a refused token = %v, want an error naming 401", err)
	}
}
