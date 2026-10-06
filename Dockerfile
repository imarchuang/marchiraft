# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/marchiraft ./cmd/marchiraft

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl \
	&& adduser -D -u 10001 marchi \
	&& mkdir -p /data \
	&& chown marchi:marchi /data
COPY --from=build /out/marchiraft /usr/local/bin/marchiraft
USER marchi
WORKDIR /data
VOLUME ["/data"]
EXPOSE 7001
ENV MARCHIRAFT_LISTEN=:7001 \
	MARCHIRAFT_DATA=/data
HEALTHCHECK --interval=10s --timeout=3s --start-period=3s --retries=3 \
	CMD curl -fsS http://127.0.0.1:7001/healthz || exit 1
ENTRYPOINT ["marchiraft"]
