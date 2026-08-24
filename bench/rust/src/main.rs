use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = env::args().nth(1).unwrap_or_else(|| {
        format!(
            "bench-rs-{}",
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        )
    });
    let count: usize = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let msg_size: usize = env::var("MSG_SIZE").ok().and_then(|s| s.parse().ok()).unwrap_or(100);
    let payload = vec![b'x'; msg_size];

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "30000")
        .set("linger.ms", "5")
        .set("acks", env::var("ACKS").unwrap_or_else(|_| "1".into()))
        .set("allow.auto.create.topics", "true")
        .create()
        .expect("producer");

    let t0 = Instant::now();
    for i in 0..count {
        let key = (i % 64).to_string();
        loop {
            match producer.send(BaseRecord::to(&topic).payload(&payload).key(&key)) {
                Ok(()) => break,
                Err((e, _)) => {
                    if format!("{e}").contains("QueueFull") {
                        producer.poll(Duration::from_millis(10));
                    } else {
                        panic!("{e}");
                    }
                }
            }
        }
        if i % 1000 == 0 {
            producer.poll(Duration::from_millis(0));
        }
    }
    producer.flush(Duration::from_secs(120)).unwrap();
    let produce_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let group = format!(
        "bench-rs-{}",
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    );
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer");
    consumer.subscribe(&[&topic]).unwrap();

    let t1 = Instant::now();
    let mut n = 0usize;
    while n < count {
        match consumer.poll(Duration::from_millis(100)) {
            None => {}
            Some(Ok(_m)) => n += 1,
            Some(Err(e)) => panic!("{e}"),
        }
    }
    let consume_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let out = serde_json::json!({
        "lib": "rdkafka-rust",
        "topic": topic,
        "count": count,
        "produce_ms": (produce_ms * 100.0).round() / 100.0,
        "consume_ms": (consume_ms * 100.0).round() / 100.0,
        "produce_msg_s": (count as f64 / (produce_ms / 1000.0)).round(),
        "consume_msg_s": (count as f64 / (consume_ms / 1000.0)).round(),
    });
    println!("{out}");
}
