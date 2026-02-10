# HFT Ingestion Engine

A high-performance implementation of an Ingestion Layer for a Crypto Backend-as-a-Service (BaaS) platform, written in Rust. This engine is designed to ingest market data from multiple exchanges via WebSocket/FIX, normalize it, and publish it to Redpanda with deterministic sub-millisecond latency.

## Key Features

- **Lock-Free Ring Buffer**: Custom SPSC implementation using `AtomicUsize` and cache-line padding, capable of **~46 million messages/sec**.
- **Dynamic Connection Management**: `BinanceConnector` automatically handles WebSocket connections, subscriptions, and reconnections.
- **Resilient Architecture**: Supports "Ingestion-Only Mode" – if Redpanda is unreachable, the system continues to ingest and process data (logging warnings) rather than crashing.
- **Low-Latency Serialization**: Optimized data models with support for `bincode` (benchmarked at ~1.4µs/op).

## Architecture Overview

The system is designed as a lock-free pipeline to minimize latency and jitter:
1. **Connectors**: Async Tokio tasks that connect to Exchanges (e.g., Binance) via WebSocket.
2. **Ingestion Channel**: High-performance MPSC channel to funnel data from multiple connectors to the normalization layer.
3. **Ring Buffer**: A custom **Lock-Free SPSC Ring Buffer** that acts as the "Disruptor" to buffer data between the Ingestion/Normalization context and the Publish context without locking.
4. **Producers**: Async tasks that publish normalized binary data to Redpanda.

## Key Components

### 1. Lock-Free Ring Buffer
Located in `src/core/ring_buffer.rs`.
- Implements a `RingBuffer<T>` using `AtomicUsize` and `UnsafeCell`.
- Uses strict **cache line padding** (64 bytes) to prevent false sharing.
- **SPSC** (Single-Producer, Single-Consumer) design optimized for the critical path.

### 2. Configuration
Located in `src/config.rs`.
- Robust configuration schema for managing Exchanges, Symbols, and Redpanda settings.
- Supports loading connections via environment variables or config files.
- **Dynamic Symbol Mapping**: Maps exchange-specific tickers (e.g., `BTCUSDT`) to internal normalized IDs (e.g., `BTC-USD`) at runtime.

### 3. Normalization & Data Model
Located in `src/model.rs`.
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

## Usage

### Prerequisites
- Rust (latest stable)
- Redpanda / Kafka (Localhost:9092 recommended, but optional for Ingestion-Only testing)

### Build
```bash
cargo build --release
```

### Run
```bash
RUST_LOG=info cargo run --release
```

The application will:
1. Load configuration (defaults to `BTC-USD` and `ETH-USD`).
2. Initialize the `RingBuffer`.
3. Connect to Binance WebSocket and subscribe to configured symbols.
4. Normalize incoming trades.
5. Publish messages to the configured Redpanda topic (or log warnings if Redpanda is down).

## Testing & Benchmarking

To run the unit tests:

```bash
cargo test
```

To run the performance benchmarks (Ring Buffer & Serialization):

```bash
cargo bench
```

### Benchmark Results (Reference)

| Component | Metric | Result |
|-----------|--------|--------|
| **Ring Buffer (Concurrent)** | Throughput | **~46 Million ops/sec** (~21.7ns/op) |
| **Ring Buffer (Single)** | Latency | ~3.5ns/op |
| **Serialization (Bincode)** | Latency | ~1.4µs/op |
| **Serialization (JSON)** | Latency | ~1.5µs/op |

## Project Structure

```
.
├── Cargo.toml
├── benches             # Performance benchmarks
│   ├── ring_buffer_bench.rs
│   └── serialization_bench.rs
├── src
│   ├── lib.rs              # Library export
│   ├── config.rs           # Configuration definitions
│   ├── connectors          # Exchange connectivity logic
│   │   └── mod.rs
│   ├── core                # Core low-latency primitives
│   │   ├── mod.rs
│   │   └── ring_buffer.rs  # Lock-free RingBuffer implementation
│   ├── main.rs             # Application entry & pipeline wiring
│   ├── model.rs            # Data models & serialization
│   ├── normalizers         # Data normalization logic
│   │   └── mod.rs
│   └── producers           # Output publication logic
│       └── mod.rs
└── README.md
```
