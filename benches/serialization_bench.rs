use chrono::Utc;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use hft_ingestion_engine::model::{MarketEventType, NormalizedMarketData};

fn serialization_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization");

    let data = NormalizedMarketData {
        symbol_id: "BTC-USD".to_string(),
        exchange: "binance".to_string(),
        event_type: MarketEventType::Trade,
        price: 98765.43,
        quantity: 0.12345,
        time_exchange: Utc::now(),
        time_ingest: Utc::now(),
        sequence: 123456789,
        is_snapshot: false,
    };

    group.bench_function("json_serialize", |b| {
        b.iter(|| {
            serde_json::to_vec(black_box(&data)).unwrap();
        })
    });

    group.bench_function("bincode_serialize", |b| {
        b.iter(|| {
            bincode::serialize(black_box(&data)).unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, serialization_benchmark);
criterion_main!(benches);
