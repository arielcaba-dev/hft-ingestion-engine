# Torii Ingestion Engine

![Torii Logo](assets/logo.png)

A high-performance implementation of an Ingestion Layer for a Crypto Backend-as-a-Service (BaaS) platform, written in Rust. This engine is designed to ingest market data from multiple exchanges via WebSocket/FIX, normalize it, and publish it to Redpanda with deterministic sub-millisecond latency.

## Key Features

- **Lock-Free Ring Buffer**: Custom SPSC implementation using `AtomicUsize` and cache-line padding, capable of **~46 million messages/sec**.
- **Dynamic Connection Management**: `BinanceConnector` automatically handles WebSocket connections, subscriptions, and reconnections.
- **Resilient Architecture**: Supports "Ingestion-Only Mode" – if Redpanda is unreachable, the system continues to ingest and process data (logging warnings) rather than crashing.
- **Low-Latency Serialization**: Optimized data models with support for `bincode` (benchmarked at ~1.4µs/op).

## Architecture Overview

The system is designed as a lock-free pipeline to minimize latency and jitter:

```
Binance WebSocket → Torii Ingestion Engine → Redpanda → QuestDB
                    (Normalize & Buffer)     (Stream)   (Time-Series DB)
```

### Pipeline Stages

1. **Connectors**: Async Tokio tasks that connect to Exchanges (e.g., Binance) via WebSocket.
2. **Ingestion Channel**: High-performance MPSC channel to funnel data from multiple connectors to the normalization layer.
3. **Ring Buffer**: A custom **Lock-Free SPSC Ring Buffer** that acts as the "Disruptor" to buffer data between the Ingestion/Normalization context and the Publish context without locking.
4. **Producers**: Async tasks that publish normalized binary data to Redpanda.
5. **Python Bridge**: Kafka consumer that writes to QuestDB via InfluxDB Line Protocol (ILP).
6. **QuestDB**: Time-series database for persistent storage and analytics.

### Current Status
- ✅ **168,637+ trades** ingested and stored
- ✅ **Sub-second latency** end-to-end
- ✅ **Real-time streaming** from Binance (BTC-USD, ETH-USD)


## Key Components

### 1. Lock-Free Ring Buffer
Located in `torii_ingestion/src/core/ring_buffer.rs`.
- Implements a `RingBuffer<T>` using `AtomicUsize` and `UnsafeCell`.
- Uses strict **cache line padding** (64 bytes) to prevent false sharing.
- **SPSC** (Single-Producer, Single-Consumer) design optimized for the critical path.

### 2. Configuration
Located in `torii_ingestion/src/config.rs`.
- Robust configuration schema for managing Exchanges, Symbols, and Redpanda settings.
- Supports loading connections via environment variables or config files.
- **Dynamic Symbol Mapping**: Maps exchange-specific tickers (e.g., `BTCUSDT`) to internal normalized IDs (e.g., `BTC-USD`) at runtime.

### 3. Normalization & Data Model
Located in `torii_ingestion/src/model.rs`.
- **Dual Timestamping**: Every event captures `time_exchange` (matching engine time) and `time_ingest` (arrival time) to track network jitter.
- **Unified ID**: Maps exchange-specific tickers to internal normalized IDs.

### 4. Connectors & Producers
- **BinanceConnector**: Production-ready WebSocket implementation using `tokio-tungstenite`.
    - Connects to `wss://stream.binance.com:9443/ws`.
    - Dynamically subscribes to symbols defined in `Settings`.
    - Deserializes JSON events into `NormalizedMarketData`.
- **RedpandaProducer**: Reliable publisher using the pure-Rust `kafka` crate.
    - Soft-fail mechanism: continues running if broker is unavailable.
    - Partitions messages by `symbol_id`.

## Deployment

### Docker Compose (Recommended)

The complete stack is containerized and can be deployed with a single command:

```bash
docker-compose up -d
```

This starts:
- **Redpanda** (Kafka-compatible broker)
- **QuestDB** (Time-series database)
- **Postgres** (Metadata storage)
- **Torii Ingestion** (Rust application)
- **QuestDB Bridge** (Python Kafka → QuestDB writer)

### Service Ports

| Service | Port | Purpose |
|---------|------|---------|
| Redpanda | 19092 | Kafka API |
| Redpanda | 18081 | Schema Registry |
| Redpanda | 19644 | Admin API |
| QuestDB | 9000 | HTTP/REST API |
| QuestDB | 9009 | InfluxDB Line Protocol (ILP) |
| QuestDB | 8812 | PostgreSQL wire protocol |
| Postgres | 5432 | PostgreSQL |

### Verification

Check service health:
```bash
docker-compose ps
```

Query QuestDB:
```bash
curl 'http://localhost:9000/exec?query=SELECT%20count()%20FROM%20trades'
```

View ingestion logs:
```bash
docker logs torii-ingestion --tail 50
```

### Manual Build (Development)

If you want to run the Rust engine standalone:

```bash
# Build
cd torii_ingestion
cargo build --release

# Run (requires Redpanda at localhost:9092)
RUST_LOG=info cargo run --release
```

The application will:
1. Load configuration (defaults to `BTC-USD` and `ETH-USD`).
2. Initialize the `RingBuffer`.
3. Connect to Binance WebSocket and subscribe to configured symbols.
4. Normalize incoming trades.
5. Publish messages to the configured Redpanda topic.


## Testing & Benchmarking

To run the unit tests:

```bash
cd torii_ingestion
cargo test
```

To run the performance benchmarks (Ring Buffer & Serialization):

```bash
cd torii_ingestion
cargo bench
```

### Benchmark Results (Reference)

| Component | Metric | Result |
|-----------|--------|--------|
| **Ring Buffer (Concurrent)** | Throughput | **~46 Million ops/sec** (~21.7ns/op) |
| **Ring Buffer (Single)** | Latency | ~3.5ns/op |
| **Serialization (Bincode)** | Latency | ~1.4µs/op |
| **Serialization (JSON)** | Latency | ~1.5µs/op |

## QuestDB Integration

### Python Bridge

A lightweight Python service (`bridge.py`) consumes normalized trades from Redpanda and writes them to QuestDB using the InfluxDB Line Protocol (ILP) over TCP.

**Key Features**:
- High-performance Kafka consumer using `confluent-kafka` (C bindings)
- Official QuestDB Python client for ILP ingestion
- Automatic timestamp conversion (Unix epoch → nanoseconds)
- Real-time flush for low-latency writes

**Performance**:
- **Throughput**: ~47 rows/second sustained
- **Latency**: ~1-5ms (Python overhead)
- **Protocol**: TCP ILP (port 9009)

**Files**:
- [`bridge.py`](bridge.py) - Kafka consumer and QuestDB writer
- [`Dockerfile.bridge`](Dockerfile.bridge) - Container image

### Data Schema

QuestDB `trades` table:
```sql
CREATE TABLE trades (
    symbol SYMBOL,
    price DOUBLE,
    quantity DOUBLE,
    timestamp TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

### Sample Queries

**Row count**:
```sql
SELECT count() FROM trades;
```

**Latest trades**:
```sql
SELECT * FROM trades ORDER BY timestamp DESC LIMIT 10;
```

**Aggregated statistics**:
```sql
SELECT symbol, count() as trades, min(price) as low, max(price) as high 
FROM trades 
GROUP BY symbol;
```


## Project Structure

```
.
├── torii_ingestion/            # Torii Ingestion Engine (Rust)
│   ├── Cargo.toml
│   ├── Dockerfile
│   ├── benches/
│   │   ├── ring_buffer_bench.rs
│   │   └── serialization_bench.rs
│   └── src/
│       ├── lib.rs
│       ├── config.rs
│       ├── connectors/
│       ├── core/
│       ├── main.rs
│       ├── model.rs
│       ├── normalizers/
│       └── producers/
├── torii_gateway/              # Torii API Gateway (Rust)
├── bridge.py                   # Python Kafka → QuestDB bridge
├── arroyo/                     # Arroyo SQL pipelines
│   ├── pipeline.sql
│   ├── pipeline_simple.sql
│   ├── pipeline_stable.sql
│   └── pipeline_questdb.sql
├── Dockerfile.bridge
└── README.md
```

## Production Metrics

**Current Deployment** (as of 2026-02-12):
- **Total Trades Ingested**: 168,637
- **BTC-USD**: 100,139 trades ($66,921 - $67,292)
- **ETH-USD**: 68,498 trades ($1,962 - $1,973)
- **Uptime**: 100%
- **Data Loss**: 0%
- **End-to-End Latency**: < 1 second

## License

MIT

