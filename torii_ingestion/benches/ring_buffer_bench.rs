use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::thread;
use torii_ingestion_engine::core::ring_buffer::RingBuffer;

fn ring_buffer_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer");

    group.bench_function("single_thread_push_pop", |b| {
        let buffer = RingBuffer::<u64>::new(1024);
        b.iter(|| {
            buffer.push(black_box(42)).unwrap();
            black_box(buffer.pop().unwrap());
        })
    });

    group.bench_function("spsc_throughput", |b| {
        b.iter_custom(|iters| {
            let buffer = Arc::new(RingBuffer::<u64>::new(65536));
            let buffer_producer = buffer.clone();
            let buffer_consumer = buffer.clone();

            let producer = thread::spawn(move || {
                for i in 0..iters {
                    while buffer_producer.push(i).is_err() {
                        // spin
                        std::hint::spin_loop();
                    }
                }
            });

            let start = std::time::Instant::now();
            let consumer = thread::spawn(move || {
                let mut count = 0;
                while count < iters {
                    if let Some(_) = buffer_consumer.pop() {
                        count += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });

            producer.join().unwrap();
            consumer.join().unwrap();
            start.elapsed()
        })
    });

    group.finish();
}

criterion_group!(benches, ring_buffer_benchmark);
criterion_main!(benches);
