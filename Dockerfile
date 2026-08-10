# Dockerfile for shoeboxd — the standalone server binary.
#
# Build:  docker build -t shoeboxd .
# Run:    docker run -p 8080:8080 shoeboxd
# Config: docker run -v $(pwd)/config.yaml:/etc/shoebox/config.yaml shoeboxd --config=/etc/shoebox/config.yaml
#
# The image defaults to memory storage. For persistence, mount a data
# volume and override the storage flags:
#
#	docker run -p 8080:8080 -v shoebox-data:/data \
#	  shoeboxd --storage=sqlite --path=/data/shoebox.db

# --- build stage ---
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Cache dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a static binary.
COPY . .
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w -X main.version=docker" \
    -o /bin/shoeboxd \
    ./cmd/shoeboxd

# --- runtime stage ---
# distroless/static: no shell, no package manager, ~2 MB. Runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /bin/shoeboxd /app/shoeboxd
COPY --from=builder /src/cmd/shoeboxd/config.example.yaml /etc/shoebox/config.example.yaml

EXPOSE 8080

USER nonroot:nonroot

# Go runtime GC tuning: without a heap cap, a message spike can push GC
# chase an unbounded heap and get OOM-killed against the container limit.
# GOMEMLIMIT is a soft cap — the GC targets it but stays correct beyond it.
# Set it to ~80-90% of the container's memory limit. Override per-deploy:
#   docker run -e GOMEMLIMIT=1GiB ...
ENV GOMEMLIMIT=512MiB

ENTRYPOINT ["/app/shoeboxd"]
CMD ["--addr=:8080", "--storage=memory"]
