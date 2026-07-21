# Spider-go


## What?

WebCrawler on golang

## Architecture
<img width="2200" height="1720" alt="изображение" src="https://github.com/user-attachments/assets/952f7c55-2325-4cfb-9196-0a47819dd9dd" />

The system is split into four processes:

- **API Gateway** (Gin, `:8080`) — the only HTTP-facing service. Validates input and forwards crawl requests to the Crawler Service over gRPC.
- **Crawler Service** (`:50051`) — the core. Runs a worker pool that fetches pages, parses HTML, checks `robots.txt`, deduplicates content, and coordinates the crawl session.
- **Queue Service** (`:50052`) — a thin gRPC wrapper around Redis. Owns one queue per domain (`crawl_queue:<domain>`) plus a set of currently active domains.
- **Storage Service** (`:50053`) — a thin gRPC wrapper around MySQL (via GORM). Owns the `pages` table and dedup lookups.

Each service is its own binary and can be deployed/restarted independently. They only know each other through their generated gRPC clients (`gen/crawler`, `gen/queue`, `gen/storage`), not through shared state.


## Tech stack

| Layer | Choice |
|---|---|
| HTTP gateway | [Gin](https://github.com/gin-gonic/gin) |
| Service-to-service | gRPC + Protocol Buffers |
| Queue | Redis (per-domain lists + a set of active domains) |
| Storage | MySQL via [GORM](https://gorm.io) |
| HTML parsing | [goquery](https://github.com/PuerkitoBio/goquery) |
| robots.txt | [temoto/robotstxt](https://github.com/temoto/robotstxt) |
| Dedup structures | [tylertreat/BoomFilters](https://github.com/tylertreat/BoomFilters) (Bloom filter), [hashicorp/golang-lru](https://github.com/hashicorp/golang-lru) |
| Logging | `log/slog`, structured JSON |

## Getting started

### Prerequisites

- Go 1.21+
- Redis
- MySQL or MariaDB
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (only needed if you change the `.proto` files)

### Setup

```bash
git clone https://github.com/ascoler/spider-go.git
cd spider-go
go mod tidy
```

Create the database:

```sql
CREATE DATABASE crawler_db;
CREATE USER 'spider'@'localhost' IDENTIFIED BY 'your-password-here';
GRANT ALL PRIVILEGES ON crawler_db.* TO 'spider'@'localhost';
```

Set the DB connection string and (optionally) a config path via environment variables:

```bash
export DB_DSN="spider:your-password-here@tcp(127.0.0.1:3306)/crawler_db?charset=utf8mb4&parseTime=True&loc=Local"
export CONFIG_PATH="./config/config.json"
```

Start Redis if it isn't already running:

```bash
redis-server
```

### Running the services

Each service is a separate binary and needs to be running for a crawl to work. Start them in this order:

```bash
go run Work_With_Db.go   # Storage Service   — :50053
go run queue.go          # Queue Service     — :50052
go run main.go           # Crawler Service   — :50051
go run Api-Gateway.go    # API Gateway       — :8080
```

### Try it

```bash
curl http://localhost:8080/health

curl -X POST http://localhost:8080/Analysis_Link \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}'
```

## Configuration

`config/config.json`:

```json
{
  "max_depth": 3,
  "max_pages": 100,
  "worker_pool_size": 10,
  "request_timeout": 10,
  "max_retries": 3,
  "retry_delay": 2,
  "rate_limit_delay": 1000,
  "storage_type": "mysql",
  "log_level": "debug",
  "output_file": "output.json"
}
```

| Field | Meaning |
|---|---|
| `max_depth` | How many hops from the seed URL(s) the crawler will follow |
| `max_pages` | Hard cap on pages processed per crawl session (can be overridden per-request) |
| `worker_pool_size` | Number of concurrent workers fetching/parsing pages |
| `max_retries` / `retry_delay` | Retry policy for queue writes (seconds between attempts) |
| `rate_limit_delay` | Minimum delay between outbound requests, in ms (see [Known limitations](#known-limitations-and-roadmap)) |

## API

**`POST /Analysis_Link`**

```json
{ "url": "https://example.com" }
```

Returns the crawled content and a status summary once the crawl session finishes (synchronous — the request blocks until the crawl completes or hits its limits).

**`GET /health`** — liveness check for the gateway.

## Project structure

```
.
├── main.go              # Crawler Service — worker pool, HTML parsing, robots.txt, dedup
├── queue.go              # Queue Service — Redis-backed per-domain queue
├── Work_With_Db.go        # Storage Service — MySQL persistence, dedup lookups
├── Api-Gateway.go         # HTTP gateway (Gin)
├── proto/                 # .proto source files
├── gen/                   # generated gRPC/protobuf code (crawler, queue, storage)
└── config/config.json      # runtime configuration
```

