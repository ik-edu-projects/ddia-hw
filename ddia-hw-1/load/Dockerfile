FROM alpine:3.24 AS builder
RUN apk add --no-cache ca-certificates go
ENV CGO_ENABLED=0 GOBIN=/out GOTOOLCHAIN=local
RUN go install github.com/rakyll/hey@v0.1.5

FROM alpine:3.24
RUN apk add --no-cache tini
COPY --from=builder /out/hey /usr/local/bin/hey
COPY run.sh /usr/local/bin/run-load.sh
RUN chmod +x /usr/local/bin/run-load.sh
ENTRYPOINT ["/sbin/tini", "-g", "--", "/usr/local/bin/run-load.sh"]
