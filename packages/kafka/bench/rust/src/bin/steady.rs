use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use rdkafka::{Offset, TopicPartitionList};
use std::env;
use std::io::{self, Write};
use std::sync::mpsc::{self, Sender};
use std::time::{Duration, Instant};

struct Delivery(Sender<Result<(), String>>);
impl ClientContext for Delivery {}
impl ProducerContext for Delivery {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        self.0
            .send(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|(err, _)| err.to_string()),
            )
            .unwrap();
    }
}

fn main() {
    let topic = env::var("BENCH_TOPIC").expect("BENCH_TOPIC");
    let size = number("MSG_SIZE", 100);
    let batch = number("BENCH_BATCH", 100);
    let windows = number("BENCH_WINDOWS", 1000);
    assert!(
        (8..=512 * 1024).contains(&size)
            && batch <= 512 * 1024 / size
            && (2..=10000).contains(&windows)
    );
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:19092".into());
    let (tx, rx) = mpsc::channel();
    let producer: ThreadedProducer<Delivery> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("acks", "1")
        .set("enable.idempotence", "false")
        .set("compression.type", "none")
        .set("linger.ms", "0")
        .set("batch.size", "1048576")
        .set("batch.num.messages", "1000000")
        .set("max.in.flight.requests.per.connection", "1")
        .set("message.timeout.ms", "30000")
        .create_with_context(Delivery(tx))
        .expect("producer");
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("{topic}-manual"))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_uncommitted")
        .set("fetch.wait.max.ms", "10")
        .set("fetch.min.bytes", "1")
        .set("fetch.queue.backoff.ms", "1")
        .set("queued.min.messages", "1")
        .set("queued.max.messages.kbytes", "1024")
        .set("fetch.max.bytes", "1048576")
        .set("max.partition.fetch.bytes", "1048576")
        .create()
        .expect("consumer");
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(&topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let payload = vec![b'x'; size];
    let keys: Vec<String> = (0..64).map(|i| i.to_string()).collect();
    let mut sent = 0usize;
    let mut received = 0usize;
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut produce = || {
        for _ in 0..batch {
            let mut value = payload.clone();
            value[..8].copy_from_slice(&(sent as u64).to_le_bytes());
            producer
                .send(
                    BaseRecord::to(&topic)
                        .partition(0)
                        .key(&keys[sent % 64])
                        .payload(&value),
                )
                .unwrap();
            sent += 1;
        }
        for _ in 0..batch {
            rx.recv_timeout(Duration::from_secs(30))
                .expect("delivery deadline")
                .expect("delivery failure");
        }
    };
    let mut consume = |target: usize| {
        while received < target {
            assert!(Instant::now() < deadline, "consumer deadline");
            if let Some(result) = consumer.poll(Duration::from_millis(10)) {
                let message = result.expect("fetch failure");
                let value = message.payload().expect("value");
                assert_eq!(message.partition(), 0);
                assert_eq!(message.offset(), received as i64);
                assert_eq!(value.len(), size);
                assert_eq!(
                    u64::from_le_bytes(value[..8].try_into().unwrap()),
                    received as u64
                );
                assert_eq!(message.key().unwrap(), keys[received % 64].as_bytes());
                assert_eq!(&value[8..], &payload[8..]);
                received += 1;
            }
        }
    };
    for _ in 0..100 {
        produce();
    }
    consume(100 * batch);
    consumer.pause(&assignment).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    emit(serde_json::json!({"phase": "start"}));
    let mut latency = Vec::with_capacity(windows);
    let started = Instant::now();
    for _ in 0..windows {
        assert!(Instant::now() < deadline, "producer deadline");
        let before = Instant::now();
        produce();
        latency.push(before.elapsed().as_secs_f64() * 1000.0);
    }
    let produce_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    consumer.resume(&assignment).unwrap();
    consume(101 * batch);
    let resume_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    consume((windows + 100) * batch);
    emit(
        serde_json::json!({"phase": "result", "lib": "rdkafka-rust", "count": windows * batch, "produce_ms": produce_ms, "consume_ms": started.elapsed().as_secs_f64() * 1000.0, "consume_count": (windows - 1) * batch, "resume_ms": resume_ms, "window_ack_ms": latency, "validated": received}),
    );
    std::thread::sleep(Duration::from_millis(100));
}

fn number(key: &str, fallback: usize) -> usize {
    let n = env::var(key)
        .map(|v| v.parse().expect("integer"))
        .unwrap_or(fallback);
    assert!(n > 0);
    n
}
fn emit(value: serde_json::Value) {
    println!("{value}");
    io::stdout().flush().unwrap();
}
