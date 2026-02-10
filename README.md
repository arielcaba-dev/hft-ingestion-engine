# HFT Ingestion Engine

A high-performance implementation of an Ingestion Layer for a Crypto Backend-as-a-Service (BaaS) platform, written in Rust. This engine is designed to ingest market data from multiple exchanges via WebSocket/FIX, normalize it, and publish it to Redpanda with deterministic sub-millisecond latency.

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
- Uses strict **cache line padding** (64 bytes) to prevent false sharing between the producer (Ingestion) and consumer (Publisher) threads.
- **SPSC** (Single-Producer, Single-Consumer) design optimized for the critical path.

### 2. Configuration
Located in `src/config.rs`.
- Robust configuration schema for managing Exchanges, Symbols, and Redpanda settings.
- Supports loading connections via environment variables or config files.

### 3. Normalization & Data Model
Located in `src/model.rs` and `src/normalizers/`.
- **Dual Timestamping**: Every event captures `time_exchange` (matching engine time) and `time_ingest` (arrival time) to track network jitter.
- **Unified ID**: Maps exchange-specific tickers (e.g., `XXBTZUSD`) to internal normalized IDs (e.g., `BTC-USD`).

### 4. Connectors & Producers
- Modular traits `ExchangeConnector` and `MessageProducer` allow easy extension.
- **BinanceConnector**: Production-ready WebSocket implementation using `tokio-tungstenite`.
    - Connects to `wss://stream.binance.com:9443/ws`.
    - Auto-subscribes to `btcusdt@trade` (configurable in code).
    - Deserializes JSON events into `NormalizedMarketData` using `serde_json`.
    - Includes automatic reconnection logic with exponential backoff strategies.- **RedpandaProducer**: Handles publishing to Redpanda topics partitioned by symbol.

## Usage

### Prerequisites
- Rust (latest stable)
- Redpanda / Kafka (optional for dry-run, required for prod)

### Build
```bash
cargo build --release
```

### Run
```bash
RUST_LOG=info cargo run --release
```

The application will:
1. Initialize the `RingBuffer`.
2. Start the `BinanceConnector` (mocked data flow in this demo).
3. Normalize incoming trades.
4. Publish mock messages to the configured Redpanda topic.

## Testing

To run the unit tests (which include a threaded stress test for the Ring Buffer):

```bash
cargo test
```

## Project Structure

```
.
├── Cargo.toml
├── src
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
