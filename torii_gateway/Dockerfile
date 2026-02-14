FROM rust:latest as builder

WORKDIR /usr/src/app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler cmake

# Copy manifest and build deps to cache them
COPY Cargo.toml Cargo.lock ./
# Create dummy main to build deps
RUN mkdir src && echo "fn main() {}" > src/main.rs
# Also create dummy build.rs if needed, but for now just src/main
RUN mkdir proto
COPY proto/market_data.proto proto/
COPY build.rs .

RUN cargo build --release

# Now copy the actual source code
COPY src src
COPY migrations migrations

# Touch main.rs to force rebuild of the app
RUN touch src/main.rs
RUN cargo build --release

# Runtime Stage
FROM debian:testing-slim

RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/app/target/release/torii_gateway .
COPY --from=builder /usr/src/app/migrations migrations

CMD ["./torii_gateway"]
