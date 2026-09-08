package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	topic := os.Getenv("BENCH_TOPIC")
	size, batch, windows := number("MSG_SIZE", 100), number("BENCH_BATCH", 100), number("BENCH_WINDOWS", 1000)
	if topic == "" || size < 8 || size > 512*1024 || batch > 512*1024/size || windows < 2 || windows > 10000 {
		panic("invalid workload")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	brokers := strings.Split(env("KAFKA_BROKERS", "127.0.0.1:19092"), ",")
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite(), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.ProducerLinger(0), kgo.ProducerBatchMaxBytes(1024*1024), kgo.MaxProduceRequestsInflightPerBroker(1), kgo.RecordDeliveryTimeout(30*time.Second), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	must(err)
	defer producer.Close()
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadUncommitted()), kgo.FetchMaxWait(10*time.Millisecond), kgo.FetchMinBytes(1), kgo.FetchMaxBytes(1024*1024), kgo.FetchMaxPartitionBytes(1024*1024))
	must(err)
	defer consumer.Close()
	payload := bytes.Repeat([]byte{'x'}, size)
	keys := make([][]byte, 64)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
	}
	sent, received := 0, 0
	produce := func() {
		done := make(chan error, batch)
		for i := 0; i < batch; i++ {
			value := bytes.Clone(payload)
			binary.LittleEndian.PutUint64(value, uint64(sent))
			producer.Produce(ctx, &kgo.Record{Topic: topic, Partition: 0, Key: keys[sent%64], Value: value}, func(_ *kgo.Record, err error) { done <- err })
			sent++
		}
		for i := 0; i < batch; i++ {
			select {
			case err := <-done:
				must(err)
			case <-ctx.Done():
				panic(ctx.Err())
			}
		}
	}
	consume := func(target int) {
		for received < target {
			fetches := consumer.PollRecords(ctx, target-received)
			for _, err := range fetches.Errors() {
				must(err.Err)
			}
			fetches.EachRecord(func(r *kgo.Record) {
				if received >= target || r.Partition != 0 || r.Offset != int64(received) || len(r.Value) != size || binary.LittleEndian.Uint64(r.Value) != uint64(received) || !bytes.Equal(r.Key, keys[received%64]) || !bytes.Equal(r.Value[8:], payload[8:]) {
					panic("record mismatch")
				}
				received++
			})
		}
	}
	for i := 0; i < 100; i++ {
		produce()
	}
	consume(sent)
	consumer.PauseFetchPartitions(map[string][]int32{topic: {0}})
	time.Sleep(50 * time.Millisecond)
	emit(map[string]any{"phase": "start"})
	latency := make([]float64, 0, windows)
	started := time.Now()
	for i := 0; i < windows; i++ {
		before := time.Now()
		produce()
		latency = append(latency, time.Since(before).Seconds()*1000)
	}
	produceMs := time.Since(started).Seconds() * 1000
	started = time.Now()
	consumer.ResumeFetchPartitions(map[string][]int32{topic: {0}})
	consume(101 * batch)
	resumeMs := time.Since(started).Seconds() * 1000
	started = time.Now()
	consume(sent)
	emit(map[string]any{"phase": "result", "lib": "franz-go", "count": windows * batch, "produce_ms": produceMs, "consume_ms": time.Since(started).Seconds() * 1000, "consume_count": (windows - 1) * batch, "resume_ms": resumeMs, "window_ack_ms": latency, "validated": received})
	time.Sleep(100 * time.Millisecond)
}

func emit(value any) { data, err := json.Marshal(value); must(err); fmt.Println(string(data)) }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func number(key string, fallback int) int {
	n, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	must(err)
	if n <= 0 {
		panic("invalid " + key)
	}
	return n
}
