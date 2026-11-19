FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gitlab-ci-job-exporter .

FROM scratch
LABEL org.opencontainers.image.title="gitlab-ci-job-exporter" \
      org.opencontainers.image.description="Prometheus exporter for GitLab CI job durations and queue times per job and runner" \
      org.opencontainers.image.source="https://github.com/praetorianer777/gitlab-ci-job-exporter" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /gitlab-ci-job-exporter /gitlab-ci-job-exporter
USER 65534:65534
EXPOSE 9168
ENTRYPOINT ["/gitlab-ci-job-exporter"]
