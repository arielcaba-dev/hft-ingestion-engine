# Torii Ingestion Engine

![Torii Logo](assets/logo.png)

A high-performance implementation of an Ingestion Layer for a Crypto Backend-as-a-Service (BaaS) platform, written in Rust. This engine is designed to ingest market data from multiple exchanges via WebSocket/FIX, normalize it, and publish it to Redpanda with deterministic sub-millisecond latency.

## Key Features

- **Lock-Free Ring Buffer**: Custom SPSC implementation using `AtomicUsize` and cache-line padding, capable of **~46 million messages/sec**.
- **Dynamic Connection Management**: `BinanceConnector` automatically handles WebSocket connections, subscriptions, and reconnections.
- **Resilient Architecture**: Supports "Ingestion-Only Mode" – if Redpanda is unreachable, the system continues to ingest and process data (logging warnings) rather than crashing.
- **Low-Latency Serialization**: Optimized data models with support for `bincode` (benchmarked at ~1.4µs/op).

## Architecture Overview

The system is designed as a lock-free pipeline to minimize latency and jitter, featuring a real-time risk engine for derived metrics:

```
Binance WebSocket → Torii Ingestion → Redpanda → Torii Gateway → WebSocket Clients
                    (Normalize)       (Stream)      (API/WS)        (JSON/Protobuf)
                                          ↓
                                    Arroyo Engine → Redpanda (metrics_derived)
                                          ↓               ↓
                                      QuestDB ←─── QuestDB Bridge (DeFi Risk Calc)
                                   (Time-Series DB)       ↑
                                          ↓          (Multi-Topic)
                                       Grafana
```

### Pipeline Stages

1. **Connectors**: Async Tokio tasks that connect to Exchanges (e.g., Binance) via WebSocket.
2. **Ingestion Channel**: High-performance MPSC channel to funnel data from multiple connectors to the normalization layer.
3. **Ring Buffer**: A custom **Lock-Free SPSC Ring Buffer** that acts as the "Disruptor" to buffer data between the Ingestion/Normalization context and the Publish context without locking.
4. **Producers**: Async tasks that publish normalized binary data to Redpanda.
5. **Arroyo Risk Engine**: SQL-based stream processing pipeline that calculates real-time indicators (RSI, Volatility) and **CVaR** using custom Rust UDFs.
6. **QuestDB Bridge**: Multi-threaded Python service that consumes from multiple topics. Crucially, it calculates **Impermanent Loss (IL)** in real-time by tracking a 60-minute rolling average price per symbol.
7. **QuestDB**: Time-series database for persistent storage and analytics.
8. **Grafana**: Provisioned dashboards for real-time visualization of market health and risk metrics.

### Current Status
- ✅ **168,637+ trades** ingested and stored
- ✅ **Real-time Risk Engine** calculating 95% CVaR
- ✅ **Sub-second latency** end-to-end
- ✅ **Grafana Dashboards** provisioned and operational
- ✅ **Real-time streaming** from Binance (BTC-USD, ETH-USD)
- ✅ **Derivatives**: Open Interest & Liquidation data from Binance Futures
- ✅ **AI-Ready**: MCP with Squeeze Analysis, Sentiment Correlation


## Key Components

### 1. Lock-Free Ring Buffer
Located in `torii_ingestion/src/core/ring_buffer.rs`.
- Implements a `RingBuffer<T>` using `AtomicUsize` and `UnsafeCell`.
- Uses strict **cache line padding** (64 bytes) to prevent false sharing.
- **SPSC** (Single-Producer, Single-Consumer) design optimized for the critical path.

### 2. Real-Time Risk Engine (Arroyo)
Located in `arroyo/risk_pipeline.sql`.
- **CVaR (Conditional Value at Risk)**: Computes the 95% worst-case return over 1-minute windows.
- **Rust UDFs**: High-performance indicators implemented in Rust and compiled into the Arroyo engine.
- **Windowed Aggregates**: Real-time OHLCV, VWAP, and Realized Volatility calculation.

### 3. Configuration
Located in `torii_ingestion/src/config.rs`.
- Robust configuration schema for managing Exchanges, Symbols, and Redpanda settings.
- Supports loading connections via environment variables or config files.
- **Dynamic Symbol Mapping**: Maps exchange-specific tickers (e.g., `BTCUSDT`) to internal normalized IDs (e.g., `BTC-USD`) at runtime.

### 4. Normalization & Data Model
Located in `torii_ingestion/src/model.rs`.
- **Dual Timestamping**: Every event captures `time_exchange` (matching engine time) and `time_ingest` (arrival time) to track network jitter.
- **Unified ID**: Maps exchange-specific tickers to internal normalized IDs.

### 5. Connectors & Producers
- **BinanceConnector**: Production-ready WebSocket implementation using `tokio-tungstenite`.
    - Connects to `wss://stream.binance.com:9443/ws`.
    - Dynamically subscribes to symbols defined in `Settings`.
    - Deserializes JSON events into `NormalizedMarketData`.
- **RedpandaProducer**: Reliable publisher using the pure-Rust `kafka` crate.
    - Soft-fail mechanism: continues running if broker is unavailable.
    - Partitions messages by `symbol_id`.

## Deployment

### Prerequisites
- Docker & Docker Compose
- Python 3.9+ (for verification scripts)

### Configuration

1.  **Environment Variables**
    Copy the sample environment file:
    ```bash
    cp .env.sample .env
    ```

    Edit `.env` and set the following:
    -   `PUBLIC_HOST`:
        -   **Local**: Input `localhost`.
        -   **Remote**: Input your server's **Public IP** or **Domain Name**.
    -   `DB_PASSWORD` / `MINIO_ROOT_PASSWORD`: Set strong passwords for production.

### Quick Start (Local & Remote)

The complete stack is containerized and can be deployed with a single command.

1.  **Start Services**
    ```bash
    docker-compose up -d --build
    ```

    This starts:
    -   **Redpanda** (Kafka-compatible broker)
    -   **QuestDB** (Time-series database)
    -   **Postgres** (Metadata storage)
    -   **Torii Ingestion** (Rust application)
    -   **Torii Gateway** (WebSocket API)
    -   **QuestDB Bridge** (Python Kafka → QuestDB writer)
    -   **Arroyo** (Stream processing cluster)
    -   **Grafana** (Visualization)
    -   **MinIO** (S3-compatible storage)

2.  **Initialize Pipeline**
    Once services are up (wait ~30s), run the initialization script to verify health, create topics, and check data flow:
    ```bash
    chmod +x init_pipeline.sh
    ./init_pipeline.sh
    ```
    *This script checks service health, produces test data, and verifies that the ingestion engine is processing live Binance data.*

3.  **Explore Documentation**
    - [API Reference](docs/API_REFERENCE.md): REST & WebSocket endpoint details.
    - [Observability Guide](docs/OBSERVABILITY.md): Grafana, QuestDB, and Arroyo monitoring.

### Remote Access

If you are deploying on a remote server (e.g., VPS):

1.  Ensure ports `8080` (Gateway), `19092` (Redpanda), and `3000` (Grafana) are open value in your firewall.
2.  Update `PUBLIC_HOST` in `.env` to your server's IP.
3.  **Verify Connectivity**:
    Run the test script from your **local machine**:
    ```bash
    # Install dependencies
    pip install websockets requests

    # Run test targeting remote host
    export GATEWAY_HOST="<YOUR_SERVER_IP>:8080"
    python3 scripts/test_gateway.py
    ```

### Service Ports

| Service | Port | Purpose |
|---------|------|---------|
| Redpanda | 19092 | Kafka API (External) |
| QuestDB | 9000 | HTTP/REST API |
| QuestDB | 9009 | InfluxDB Line Protocol (ILP) |
| Torii Gateway | 8080 | WebSocket API |
| Grafana | 3000 | Dashboards |
| Arroyo | 5115 | Arroyo UI/API |
| MinIO | 9001 | S3 Console |

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

### Multi-Topic Bridge

A high-performance Python service (`bridge.py`) acts as the ingestion backbone for QuestDB. It handles concurrent consumption from multiple Redpanda topics using a threaded architecture.

**Supported Topics**:
- `market_data_raw`: Raw trade events
- `ohlcv_1m`: 1-minute OHLCV candles
- `metrics_derived`: Real-time risk metrics (CVaR, RSI, etc.)

**Performance**:
- **Throughput**: ~47 rows/second sustained
- **Latency**: ~1-5ms (Python overhead)
- **Protocol**: TCP ILP (port 9009)

**Files**:
- [`scripts/bridge.py`](scripts/bridge.py) - Kafka consumer and QuestDB writer
- [`Dockerfile.bridge`](Dockerfile.bridge) - Container image (references `scripts/bridge.py`)

### Data Schema

QuestDB `market_risk` table:
```sql
CREATE TABLE market_risk (
    symbol SYMBOL,
    volatility DOUBLE,
    liquidity DOUBLE,
    rsi DOUBLE,
    cvar_95 DOUBLE,
    timestamp TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

QuestDB `trades` table:
```sql
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

QuestDB `defi_risk` table:
```sql
CREATE TABLE defi_risk (
    symbol SYMBOL,
    il_score DOUBLE,
    entry_price DOUBLE,
    current_price DOUBLE,
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

## WebSocket Gateway (Torii Gateway)

The `torii-gateway` service provides real-time WebSocket endpoints for streaming market data to clients.

### Endpoints

**DS Mode (Protobuf)**: `/v1/ws/ds?api_key=<key>`
- High-performance binary Protobuf stream for HFT clients.

**Standard Mode (JSON)**: `/v1/ws?api_key=<key>`
- JSON-based WebSocket stream with dynamic symbol subscriptions.

### Security & Billing Layer

The Gateway is protected by a multi-layered security and billing system:

- **Tier-Based Access Control**:
    - **Free (Tier 1)**: Standard JSON WebSocket & REST API.
    - **Pro (Tier 2)**: Higher rate limits and L3 data.
    - **Enterprise (Tier 3)**: Required for **DS Mode (Protobuf)** high-frequency streams.
- **Sliding Window Rate Limiting**: Redis-backed rate limiting with dynamic limits per tier and `X-RateLimit-*` headers.
- **Credit-Based Billing**:
    - Atomic credit deduction per request.
    - Background synchronization from Redis to Postgres every 10 seconds.
    - Automatic rejection (402 Payment Required) when credits are exhausted.
- **API Key Management**: Secure hashing (SHA256) and scope-based authorization.


## Project Structure

```
├── torii_ingestion/            # Ingestion Engine (Rust)
├── torii_gateway/              # API Gateway (Rust)
├── scripts/                    # Utilities, bridges, and tests
│   ├── bridge.py               # Multi-topic QuestDB bridge
│   ├── submit_pipeline.py      # Arroyo submission script
│   └── test_historical.py      # Historical data integration test
├── arroyo/                     # Arroyo SQL pipelines & UDFs
│   ├── risk_pipeline.sql       # Production risk pipeline
│   └── udf_indicators.rs       # Rust UDF implementations
├── docs/                       # Project Documentation
├── docker-compose.yaml         # Full-stack orchestration
└── README.md
```

## Production Metrics

**Current Deployment** (as of 2026-02-17):
- **Total Trades Ingested**: 168,637+
- **Risk Calculation Uptime**: 100%
- **Metrics Accuracy**: Verified via `test_risk_pipeline.py`
- **CVaR 95% Coverage**: Operational
- **End-to-End Latency**: < 1 second

**WebSocket Endpoints**:
- ✅ DS Mode (Protobuf) - Operational
- ✅ Standard Mode (JSON) - Operational
- ✅ rdkafka migration complete

**Services**:
- `torii-ingestion` - Binance → Redpanda ingestion
- `torii-gateway` - WebSocket API gateway
- `bridge.py` - QuestDB writer
- `redpanda` - Message broker
- `questdb` - Time-series database

## License

MIT
