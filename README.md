# gitlab-ci-job-exporter

A Prometheus exporter for GitLab CI. It reads the finished jobs of all
projects in a GitLab group and serves how long each job ran and how long it
waited for a runner, per job and per runner.

GitLab's runner exposes metrics about the runner itself, such as concurrency
and job counts. It does not tell you how long a given job takes over time, or
how long jobs queue on a given runner. This exporter reads that from the
GitLab REST API instead. It is read-only and uses the standard library only.

## Metrics

| Metric | Type | Description |
| --- | --- | --- |
| `ci_job_duration_seconds` | histogram | How long a finished job ran. |
| `ci_job_queued_seconds` | histogram | How long a finished job waited for a runner. |
| `ci_jobs_pending` | gauge | Jobs waiting for a runner, per `project` and `tags`. |
| `ci_job_pending_oldest_seconds` | gauge | Age of the oldest waiting job per `project` and `tags`: the time since its `created_at`, computed at scrape time. |
| `ci_pipelines_pending` | gauge | Pipelines with status `pending` (no job has found a runner yet), per `project`. |
| `ci_pipelines_running` | gauge | Pipelines with status `running`, per `project`. |
| `ci_pipeline_duration_seconds` | histogram | How long a finished pipeline took from its creation to its end, waiting included: what someone waiting on it sees. |
| `ci_pipeline_queued_seconds` | histogram | How long a finished pipeline waited before its first job started. |
| `ci_default_branch_failed` | gauge | 1 when the newest finished pipeline of the project's default branch failed, else 0, per `project`. |
| `ci_project_open_issues` | gauge | Open issues per `project`, from the project list; a project with issues disabled has no series. |
| `ci_project_open_merge_requests` | gauge | Open merge requests per `project`, from the `X-Total` header of the merge request list; a project for which GitLab sends no total and whose list hit the page limit has no series. |
| `ci_merge_request_oldest_open_seconds` | gauge | Age of the oldest open merge request that is not a draft, per `project`: the time since its `created_at`, computed at scrape time. A project with no such merge request has no series. |
| `ci_merge_requests_open_by_state` | gauge | Open merge requests per `project` and `state`, the value of GitLab's `detailed_merge_status` passed through and cut at 32 characters, for example `mergeable`, `conflict`, `ci_must_pass`, `ci_still_running`, `need_rebase`, `draft_status`, `not_approved`, `discussions_not_resolved`, `checking`. A GitLab that sends no status gives `unknown`. Drafts are included. |
| `ci_merge_request_time_to_merge_seconds` | histogram | How long a merged merge request took from its creation to its merge, per `project`. Each merge request is counted once, by id. |
| `ci_job_retried_total` | counter | Finished jobs that were later retried, per `project`, `job` and `ref`. Each job run is counted once. |
| `ci_job_retry_passed_total` | counter | Jobs that failed and whose retry in the same pipeline succeeded, per `project`, `job` and `ref`. Counted once per job and pipeline, however many retries it took. |
| `ci_test_failures_total` | counter | Failed test cases of failed pipelines, per `project`, `suite` and `test`. Each pipeline is counted once. |
| `ci_milestone_issues` | gauge | Issues of an active group milestone, per `milestone` and `state` (`opened` or `closed`). |
| `ci_milestone_due_timestamp_seconds` | gauge | Due date of an active group milestone as a Unix time, per `milestone`; no series for a milestone without a due date. |
| `ci_issues_unassigned` | gauge | Open issues without an assignee per `project`; a project with issues disabled has no series. |
| `ci_project_storage_bytes` | gauge | Storage a project uses, per `project` and `kind`, from GitLab's project statistics. A project for which GitLab sends no statistics has no series. |
| `ci_job_exporter_build_info` | gauge | Always 1; the `version` label is the release version. |
| `ci_job_exporter_last_poll_timestamp_seconds` | gauge | When GitLab was last read without error; 0 before the first success. |
| `ci_job_exporter_last_poll_duration_seconds` | gauge | How long the last finished poll took. |
| `ci_job_exporter_poll_errors_total` | counter | Polls that failed for at least one project. |
| `ci_job_exporter_projects_skipped_total` | counter | Project reads GitLab refused with 403 or 404; these are skipped, not errors. |
| `ci_job_exporter_page_limit_hits_total` | counter | Project reads stopped at the limit of 20 pages of jobs. |

The two histograms carry these labels:

| Label | Meaning |
| --- | --- |
| `project` | Full project path including groups, GitLab's `path_with_namespace`, for example `my-group/sub/my-project`. |
| `job` | Job name. A parallel suffix such as `: [1/2]` or ` 1/2` is removed. |
| `part` | The parallel part, for example `1/2`; empty if the job is not parallel. Cut at 64 characters. |
| `runner` | Runner description, or `none` if no runner took the job. |
| `status` | `success`, `failed` or `canceled`. |
| `ref` | `default` (the project's default branch), `merge_request` or `branch`. |

The two pending gauges carry `project` as above and `tags`: the job's runner
tags sorted and joined with `,` (for example `heavy,light`), empty if the job
has none, cut at 128 characters. They show which runner pool is short. Each
good poll replaces the whole set, so a project with no waiting jobs has no
series; a failed poll keeps the previous set. Age is measured from
`created_at`, not from GitLab's `queued_duration`, which is only set once a
runner has taken the job.

The two pipeline gauges carry only `project`. They are read from
`projects/:id/pipelines?status=pending` and `?status=running` and replaced as a
whole after each good poll, like the pending jobs: a project with no such
pipeline has no series, and a failed poll keeps the previous set.

The two pipeline histograms carry `project`, `status` (`success`, `failed`,
`canceled` or `skipped`) and `ref`, which here can also be `tag`. They are read
from `projects/:id/pipelines?scope=finished&updated_after=...` and then each
pipeline on its own, since the list carries no times. A retried pipeline that
finishes again is counted again, once per finish. `ci_default_branch_failed`
is replaced after each good poll like the gauges above.

### Merge requests

The open merge requests of each project are read as a list, oldest first, up
to 20 pages of 100. `ci_project_open_merge_requests` still comes from the
`X-Total` header (or the length of the list when GitLab sends none). When the
list hits the page limit the project has no `ci_merge_requests_open_by_state`
series, since the counts would be too low, and the page limit counter goes up;
the oldest-merge-request age stays right, as the list starts with the oldest.
The three gauges are replaced as a whole after each good poll and a failed poll
keeps the previous set.

The time to merge is read from `merge_requests?state=merged&updated_after=...`
for the same window as the finished jobs and observed once per merge request
id, from `created_at` to `merged_at`. Only merge requests merged since the
window start count. Buckets, in seconds: 600, 1800, 3600, 7200, 14400, 28800,
43200, 86400, 172800, 345600, 604800, 1209600 (ten minutes to 14 days).

### Retried jobs and failing tests

`ci_job_retried_total` and `ci_job_retry_passed_total` carry `project`, `job`
(the name without a parallel suffix such as `: [1/2]`) and `ref`
(`default`, `merge_request` or `branch`). A run counts as retried when a newer
run of the same job name exists in the same pipeline, or when GitLab marks it
`retried`. The exporter remembers the newest run of each job and pipeline, so a
retry that finishes polls later still counts the earlier run, as long as that run
is not older than the window plus an hour. The job list is read with
`include_retried=true`. The duration and queue histograms are unchanged: they
skip a run that GitLab marks as retried, and otherwise count every run as
before.

`ci_job_retry_passed_total` says how often a retry rescued a failure. It needs
the pipeline id and name of the failed run and of the retry, so it misses a
failure whose retry comes more than the window plus an hour after it.

`ci_test_failures_total` needs `artifacts:reports:junit` in the CI of the
projects; a pipeline without a test report adds nothing. For each newly
finished pipeline with status `failed` the exporter reads its test report
summary, and only when it lists test cases the full report. Failed cases
(status `failed` or `error`) are counted under `suite`, the test suite name, and
`test`, the class name and test name joined with a `.`, each cut at 128
characters. At most 50 failed cases of a pipeline are counted, and the exporter
keeps at most 2000 test series; a series that saw no failure for `SERIES_TTL`
is dropped like the job series.

### Milestones, unassigned issues and storage

`ci_milestone_issues` and `ci_milestone_due_timestamp_seconds` come from the
active milestones of the group (`groups/:group/milestones?state=active`, the
first 100) and `groups/:group/issues_statistics?milestone=<title>`, which counts
the issues of the group and its subgroups. `milestone` is the title, cut at 128
characters; two milestones with the same title count as one. `ci_issues_unassigned`
is the `X-Total` header of `projects/:id/issues?state=opened&assignee_id=None`.
All three are replaced as a whole after each good poll, and a read GitLab
refuses with 403 or 404 gives no series.

`ci_project_storage_bytes` takes its values from `projects/:id?statistics=true`,
one read per project, since GitLab 19 sends `statistics: null` in the group's
project list. GitLab sends them only to a token with at least the Reporter
role; a refused read gives no storage series and does not skip the project. `kind` is `repository`, `wiki`, `lfs`, `job_artifacts`,
`pipeline_artifacts`, `packages`, `snippets`, `uploads` or `container_registry`;
a kind GitLab leaves out has no series, and there is no total, so use
`sum by (project)`.

Job buckets, in seconds: 30, 60, 120, 180, 300, 450, 600, 900, 1200, 1800, 2700, 3600.

Pipeline buckets, in seconds: 60, 180, 300, 600, 900, 1200, 1800, 2700, 3600, 5400, 7200, 10800.

## Configuration

All settings are environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `GITLAB_URL` | required | Base URL of the GitLab instance, for example `https://gitlab.example.com`. Must be `https` unless `ALLOW_INSECURE_HTTP=true`. |
| `ALLOW_INSECURE_HTTP` | `false` | Set to `true` to accept an `http` GitLab URL. The token then travels in clear text. |
| `GITLAB_GROUP` | required | Group path or ID. Subgroups are included, archived projects and projects scheduled for deletion are not. |
| `GITLAB_TOKEN_FILE` | required | Path to a file that contains the access token. |
| `INTERVAL` | `2m` | Time between polls; at least `10s`. |
| `LOOKBACK` | `15m` | How far back the first poll looks for finished jobs; greater than zero. No poll reaches back further than 6 hours. |
| `SERIES_TTL` | `48h` | A series that saw no finished job for this long is dropped. |
| `LISTEN` | `:9168` | Address to serve `/metrics` on. |

## Token

Create a group or project access token with the role Reporter and only the
scope `read_api`. The exporter never writes to GitLab. Reporter is also what
GitLab requires to send the storage statistics. Put the token in a
file and point `GITLAB_TOKEN_FILE` at it; the token is not read from an
environment variable on purpose. The file is read once at start, so restart
the exporter after you rotate the token. The exporter refuses to start if the
file is empty or holds control characters, and it never follows a redirect, so
the token is only ever sent to `GITLAB_URL`.

## Install

Each release at <https://github.com/praetorianer777/gitlab-ci-job-exporter/releases>
has static binaries for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64
and windows/amd64, named `gitlab-ci-job-exporter_<version>_<os>_<arch>.tar.gz` (`.zip`
on Windows), each with `LICENSE` and `README.md`, and a `checksums.txt` with
SHA-256 sums. The container image for linux/amd64 and linux/arm64 is
`ghcr.io/praetorianer777/gitlab-ci-job-exporter`, tagged with the version and
`latest` (pre-releases such as `v1.0.0-rc.1` get no `latest`). Pin the version
tag in production.

## Run

With Docker:

```sh
docker run -d --name gitlab-ci-job-exporter -p 127.0.0.1:9168:9168 \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -e GITLAB_URL=https://gitlab.example.com \
  -e GITLAB_GROUP=my-group \
  -e GITLAB_TOKEN_FILE=/run/secrets/gitlab_token \
  -v "$PWD/gitlab_token":/run/secrets/gitlab_token:ro \
  ghcr.io/praetorianer777/gitlab-ci-job-exporter:v0.2.0
```

The image is built from `scratch` and runs as user 65534, so the token file
must be readable by that user.

Ready-to-use files are in [`examples/`](examples):

- [`examples/docker-compose.yml`](examples/docker-compose.yml): the exporter with the token as a Docker secret.
- [`examples/prometheus.yml`](examples/prometheus.yml): a Prometheus scrape config.
- [`examples/alloy/config.alloy`](examples/alloy/config.alloy): Grafana Alloy scraping the exporter and writing to a Prometheus or Mimir remote-write URL, with an optional `X-Scope-OrgID` header.
- [`examples/grafana/dashboard.json`](examples/grafana/dashboard.json): a small Grafana dashboard with a table per project and job (average and p90 duration, average queue time, runs, failure rate) a stat panel with the pipelines waiting, and graphs for duration, queue time per runner, failures and waiting jobs by runner tag. Further panels show open issues and merge requests, the oldest open merge request, merge requests by state, time to merge (p50 and p90), the most retried jobs, the most failing tests, milestone progress, unassigned issues and storage per project. Import it in Grafana and pick the Prometheus data source.

## Queries

Average duration per job:

```promql
sum by (project, job) (rate(ci_job_duration_seconds_sum[1d]))
/
sum by (project, job) (rate(ci_job_duration_seconds_count[1d]))
```

90th percentile of the duration per job:

```promql
histogram_quantile(0.9, sum by (project, job, le) (rate(ci_job_duration_seconds_bucket[1d])))
```

Average duration per runner:

```promql
sum by (runner) (rate(ci_job_duration_seconds_sum[1d]))
/
sum by (runner) (rate(ci_job_duration_seconds_count[1d]))
```

Average waiting time per runner:

```promql
sum by (runner) (rate(ci_job_queued_seconds_sum[1d]))
/
sum by (runner) (rate(ci_job_queued_seconds_count[1d]))
```

Waiting jobs per runner tag:

```promql
sum by (tags) (ci_jobs_pending)
```

Pipelines waiting for a runner, all projects:

```promql
sum(ci_pipelines_pending)
```

90th percentile of the time from push to a finished merge request pipeline:

```promql
histogram_quantile(0.9, sum by (project, le) (rate(ci_pipeline_duration_seconds_bucket{ref="merge_request"}[1d])))
```

Share of failed pipelines per project:

```promql
sum by (project) (rate(ci_pipeline_duration_seconds_count{status="failed"}[1d]))
/
sum by (project) (rate(ci_pipeline_duration_seconds_count[1d]))
```

Default branches that are red, for an alert with a `for` of an hour:

```promql
ci_default_branch_failed == 1
```

Median time to merge per project over a week:

```promql
histogram_quantile(0.5, sum by (project, le) (increase(ci_merge_request_time_to_merge_seconds_bucket[7d])))
```

Share of failed jobs that a retry rescued:

```promql
sum(increase(ci_job_retry_passed_total[7d])) / sum(increase(ci_job_retried_total[7d]))
```

Milestone progress in percent:

```promql
100 * sum by (milestone) (ci_milestone_issues{state="closed"}) / sum by (milestone) (ci_milestone_issues)
```

Longest wait per runner tag:

```promql
max by (tags) (ci_job_pending_oldest_seconds)
```

## Limitations

- It counts only jobs that finish while it runs, plus the jobs that finished
  within `LOOKBACK` before it started. Older history is not imported.
- Each poll costs, per project, four list reads (finished and waiting jobs,
  pending and running pipelines), two more for finished pipelines and the
  default branch, and one read per pipeline that finished since the last poll.
  The open merge requests are read as a list: one read per 100 of them, at
  most 20, where a one-item page was enough before. Merged merge requests
  since the last poll cost at least one more read, open issues without an
  assignee one small read (none for a project with issues disabled), and each
  newly finished failed pipeline one read of its test report summary and, when
  it has tests, one of the full report. Once per poll the group's active
  milestones cost one read and one more per milestone. Each project's
  statistics cost one more small read. The
  open issue count comes with the project list at no extra cost.
- The counters live in memory and restart from zero with the process. Use
  `rate` or `increase`, which handle resets.
- A job that finishes between two polls is seen at the next poll, so the
  metrics lag by up to `INTERVAL`.
- A project that cannot be read is skipped for that poll and counted in
  `ci_job_exporter_poll_errors_total`. A project GitLab answers with 403 or
  404 is skipped quietly and counted in `ci_job_exporter_projects_skipped_total`.
  A 429 or 5xx answer ends the poll, after waiting out a `Retry-After` of up
  to 30 seconds.
- After a failed poll the window is kept, so the next poll still covers the
  jobs the failed one missed, up to 6 hours back.
- At most 20 pages of 100 are read per project and poll, for finished jobs, waiting jobs, each pipeline status, and open and merged merge requests.
- A runner description that changes shows up as a new `runner` label value.

## Upgrading to 0.2.0

- The `project` label is now the full path with groups (`my-group/my-project`),
  not the last path segment. Existing series are replaced by new ones, so
  update queries and alerts that match on `project`.
- `GITLAB_URL` must be `https` unless `ALLOW_INSECURE_HTTP=true`, `INTERVAL`
  must be at least `10s` and the token file must not be empty.

## Build and test

Go 1.26 is required.

```sh
go vet ./...
go test -race ./...
go build -o gitlab-ci-job-exporter .
docker build -t gitlab-ci-job-exporter .
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
