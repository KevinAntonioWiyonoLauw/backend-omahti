# Dockerfile untuk backend demo. Build multi-stage: image akhir hanya berisi binary.
FROM golang:1.24-alpine AS build

WORKDIR /src

# Layer dependency dipisah supaya cache build tidak batal setiap kali kode berubah.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM alpine:3.20

RUN adduser -D -H -u 10001 app
WORKDIR /app

COPY --from=build /out/server /app/server
COPY web /app/web

USER app
EXPOSE 8000

# Health check memakai endpoint /api/health yang juga mengecek Redis.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
  CMD wget -qO- http://127.0.0.1:8000/api/health >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/app/server"]
