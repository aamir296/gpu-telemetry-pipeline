# syntax=docker/dockerfile:1.7
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS=linux
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} sh -c 'for service in queue streamer collector api migrate retention; do go build -trimpath -ldflags="-s -w" -o /out/${service} ./cmd/${service}; done'

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/ /app/
ARG INPUT_CSV=input/real-dcgm.csv
COPY ${INPUT_CSV} /data/input.csv
USER 65532:65532
ENTRYPOINT ["/app/api"]
