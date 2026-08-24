package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := env("KAFKA_BROKERS", "127.0.0.1:9092")
	topic := arg(1, fmt.Sprintf("bench-go-%d", time.Now().UnixNano()))
	count := atoi(arg(2, "10000"))
	payload := make([]byte, atoi(env("MSG_SIZE", "100")))
	for i := range payload {
		payload[i] = 'x'
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.DefaultProduceTopic(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	must(err)

	ctx := context.Background()
	t0 := time.Now()
	for i := 0; i < count; i++ {
		r := &kgo.Record{Key: []byte(strconv.Itoa(i % 64)), Value: payload, Topic: topic}
		cl.Produce(ctx, r, func(_ *kgo.Record, err error) { must(err) })
	}
	must(cl.Flush(ctx))
	produceMs := time.Since(t0).Seconds() * 1000
	cl.Close()

	cl, err = kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		}),
	)
	must(err)
	defer cl.Close()

	t1 := time.Now()
	n := 0
	for n < count {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != nil {
					panic(e.Err)
				}
			}
		}
		fetches.EachRecord(func(r *kgo.Record) { n++ })
	}
	consumeMs := time.Since(t1).Seconds() * 1000

	out, _ := json.Marshal(map[string]any{
		"lib":           "franz-go",
		"topic":         topic,
		"count":         count,
		"produce_ms":    round(produceMs),
		"consume_ms":    round(consumeMs),
		"produce_msg_s": round(float64(count) / (produceMs / 1000)),
		"consume_msg_s": round(float64(count) / (consumeMs / 1000)),
	})
	fmt.Println(string(out))
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func arg(i int, d string) string {
	if len(os.Args) > i {
		return os.Args[i]
	}
	return d
}
func atoi(s string) int { n, _ := strconv.Atoi(s); return n }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func round(f float64) float64 { return float64(int(f*100+0.5)) / 100 }
