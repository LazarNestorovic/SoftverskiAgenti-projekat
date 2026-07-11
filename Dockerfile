FROM golang:1.26-alpine AS builder
WORKDIR /app

# Preuzmi zavisnosti
COPY go.mod go.sum ./
RUN go mod download

# Kopiraj kod i kompajliraj
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o fl-demo ./demo/

# ── Runtime image ──────────────────────────────────────────────────────────────
FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app

COPY --from=builder /app/fl-demo .
COPY demo/config.yaml .
# housing.csv se montira kao volume (nije deo slike — 1.4MB CSV)

ENTRYPOINT ["./fl-demo"]
CMD ["--config=config.yaml"]
