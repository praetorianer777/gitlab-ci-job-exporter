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

Buckets, in seconds: 30, 60, 120, 180, 300, 450, 600, 900, 1200, 1800, 2700, 3600.

## Configuration

All settings are environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `GITLAB_URL` | required | Base URL of the GitLab instance, for example `https://gitlab.example.com`. Must be `https` unless `ALLOW_INSECURE_HTTP=true`. |
| `ALLOW_INSECURE_HTTP` | `false` | Set to `true` to accept an `http` GitLab URL. The token then travels in clear text. |
| `GITLAB_GROUP` | required | Group path or ID. Subgroups are included, archived projects are not. |
| `GITLAB_TOKEN_FILE` | required | Path to a file that contains the access token. |
| `INTERVAL` | `2m` | Time between polls; at least `10s`. |
| `LOOKBACK` | `15m` | How far back the first poll looks for finished jobs; greater than zero. No poll reaches back further than 6 hours. |
| `SERIES_TTL` | `48h` | A series that saw no finished job for this long is dropped. |
| `LISTEN` | `:9168` | Address to serve `/metrics` on. |

## Token

Create a group or project access token with the role Reporter and only the
scope `read_api`. The exporter never writes to GitLab. Put the token in a
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
- [`examples/grafana/dashboard.json`](examples/grafana/dashboard.json): a small Grafana dashboard with a table per project and job (average and p90 duration, average queue time, runs, failure rate) and graphs for duration, queue time per runner, failures and waiting jobs by runner tag. Import it in Grafana and pick the Prometheus data source.

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

Longest wait per runner tag:

```promql
max by (tags) (ci_job_pending_oldest_seconds)
```

## Limitations

- It counts only jobs that finish while it runs, plus the jobs that finished
  within `LOOKBACK` before it started. Older history is not imported.
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
- At most 20 pages of 100 jobs are read per project and poll, for finished and for waiting jobs each.
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
