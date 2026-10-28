# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/raftkv    ./cmd/raftkv \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/raftkvctl ./cmd/raftkvctl

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
 && adduser -D -u 10001 -h /data raftkv
COPY --from=build /out/raftkv /out/raftkvctl /usr/local/bin/
USER raftkv
WORKDIR /data
VOLUME /data
EXPOSE 7000
ENV RAFTKV_LISTEN=:7000 RAFTKV_DATA_DIR=/data
ENTRYPOINT ["/usr/local/bin/raftkv"]
