---
title: Chaos testing
description: Broker failure and chaos testing
---

## Status

**Current status: the full chaos suite passes and results are recorded in `out/chaos/`.**

Run the deterministic mock-broker suite with `bun run test:chaos:mock`. Run the full three-broker Docker suite, including `tc netem` profiles, with `bun run test:chaos` (requires the host `fs.aio-max-nr` sysctl to be at least 66563 for the Redpanda containers). Set `CHAOS_NETEM=0` to omit those profiles or `CHAOS_TLS_EXTERNAL=1` to include the external expired-certificate check. Results are written to `out/chaos/`.

The client reconnects a closed TCP connection when a later request uses that connection. Active requests reject when the socket closes. Retriable Produce and Fetch failures use bounded retries and refresh leader metadata. Set producer `idempotent: true` to attach broker-managed sequence state; the default non-idempotent mode can create duplicates after a lost response.

Therefore, the current safe promise is **bounded failure**, not seamless recovery.

## Current expected behavior

| Event                                       | Current expected result                                                             |
| ------------------------------------------- | ----------------------------------------------------------------------------------- |
| Bootstrap broker is unavailable             | Metadata tries the next configured or known broker.                                 |
| Active socket closes                        | All active requests on that socket reject.                                          |
| Same broker returns before the next request | A later request can open a new socket.                                              |
| Partition leader moves                      | Produce and Fetch refresh metadata and retry within the configured budget.          |
| Produce response is lost                    | Idempotent mode reuses the partition sequence; default mode can create a duplicate. |
| Broker accepts Produce but response is lost | The Promise rejects or times out; the record can still exist.                       |
| Consumer Fetch fails                        | `fetch()` retries retriable failures, then rejects when its budget is exhausted.    |
| Connected broker stays silent               | The request rejects after `requestTimeoutMs`.                                       |
| New TCP connection is blackholed            | `connectTimeoutMs` rejects the connection attempt.                                  |
| Response is malformed or too large          | The connection rejects the response.                                                |

Do not describe idempotent Produce as qualified until its broker-failure chaos gates pass.

## Safety rules

- Use a dedicated test cluster.
- Use synthetic data only.
- Set a hard test timeout.
- Save broker and client logs.
- Use unique topic names for every test.
- Stop all fault injection in test cleanup.
- Do not run `iptables`, `tc`, or broker kill commands on a production host.

Use at least three brokers for leader movement, quorum, and rolling-restart tests. A one-broker container is sufficient only for connection reset and timeout tests.

## Data oracle

Every produced message must contain:

- Test run ID
- Topic and partition
- Monotonic sequence number
- Producer attempt number
- Stable message ID
- Payload checksum

After each test, scan all records and classify each message ID as:

- Acknowledged and present once
- Acknowledged and missing
- Rejected and absent
- Rejected but present
- Present more than once
- Out of order within a partition
- Corrupt

The classification is necessary because a lost response creates an unknown Produce result. In default non-idempotent mode, **rejected but present** and duplicates can occur after an application retry.

## Fault tools

Use one or more of these tools:

- Docker `stop`, `kill`, `pause`, and `unpause`
- Redpanda `rpk` or Kafka administration tools for leader changes
- Linux `tc netem` for delay, loss, duplication, and bandwidth limits
- Linux firewall rules for blackholes and connection rejection
- A TCP fault proxy for reset, truncation, and malformed-frame tests
- The Bun mock broker for deterministic protocol faults

Fault tools are test dependencies. They are not runtime dependencies of bun-kafka.

## Test phases

Each chaos test has five phases:

1. **Warm:** Produce and consume normally for at least one minute.
2. **Inject:** Apply one fault at a recorded time.
3. **Observe:** Keep traffic active while the fault exists.
4. **Recover:** Remove the fault or start the broker.
5. **Verify:** Drain the topic and run the data oracle.

Record request latency, failures, memory, sockets, offsets, and broker leadership through all phases.

## Required scenarios

### 1. Bootstrap broker unavailable

Setup:

- Configure at least three bootstrap addresses.
- Stop the first bootstrap broker before client startup.

Pass conditions:

- Metadata uses another configured broker.
- A refused connection moves to the next broker without an application retry.
- A blackholed connection rejects within `connectTimeoutMs` plus test tolerance.
- No request remains pending after the connection attempt ends.
- No extra socket remains open to the failed address.

### 2. Producer leader process is killed

Setup:

- Start continuous acknowledged production.
- Kill the partition leader during active Produce requests.

Pass conditions for the current client:

- Every affected Promise completes or rejects within `requestTimeoutMs` plus 1 second.
- No Promise remains pending.
- The producer queue does not grow after requests reject.
- `close()` does not hang.
- The data oracle reports unknown results separately.

Expected current limitation:

- A later send can still fail when the retry budget is exhausted or the broker does not recover.

Future seamless-recovery gate:

- Preserve idempotent sequence state through leader failover and broker restart.

### 3. Consumer leader process is killed

Setup:

- Assign partitions manually.
- Consume continuously.
- Kill one partition leader during Fetch.

Pass conditions for the current client:

- The active `fetch()` rejects within the timeout.
- Already returned zero-copy values remain readable.
- No offset advances for records that were not returned.
- Seek to the last processed offset works after recovery.

Expected current limitation:

- The caller must handle the error when the retry budget is exhausted.

### 4. TCP reset during Produce response

Inject a reset after the broker receives bytes but before the client receives the full response.

Pass conditions:

- The request rejects.
- The response parser does not accept a partial frame.
- The next connection starts with a clean frame state.
- The data oracle records whether the message reached the broker.

This scenario must not assert that every rejected record is absent.

### 5. TCP reset during Fetch response

Inject a reset in the middle of a large record-set response.

Pass conditions:

- The partial response is discarded.
- No partial Kafka message is returned.
- Existing messages from earlier responses remain valid.
- Resident memory returns to its normal range.
- A later request does not use bytes from the failed frame.

### 6. Network blackhole

Drop packets without sending a reset for longer than `requestTimeoutMs`.

Pass conditions:

- Requests reject by timeout.
- Timeout error count equals the affected request count.
- Shutdown remains bounded.
- Memory and pending-request counts return to normal after rejection.

Run this test for Produce, Fetch, Metadata, and ListOffsets on an established connection. Run a separate startup blackhole test for `connectTimeoutMs`.

### 7. Slow and lossy network

Run these profiles with `tc netem`:

| Profile            |  Delay | Jitter | Loss |
| ------------------ | -----: | -----: | ---: |
| Local degradation  |  10 ms |   2 ms | 0.1% |
| Regional network   |  50 ms |  10 ms | 0.5% |
| Severe degradation | 200 ms |  50 ms |   2% |

Pass conditions:

- Latency rises as expected, without unbounded queue growth.
- Requests either complete or time out.
- The client does not enter a reconnect loop without delay.
- CPU does not stay at 100% while the network is unavailable.

### 8. Rolling broker restart

Restart one broker at a time while traffic continues on a three-broker cluster.

Current release gate:

- Failures are bounded and visible.
- The application can recover by recreating failed operations or clients.
- Acknowledged records are not missing.

Future seamless-recovery gate:

- Produce and Fetch continue after automatic metadata refresh without application recreation.

### 9. Leader transfer without broker death

Move leadership to another broker while the old leader stays online.

Pass conditions:

- `NOT_LEADER_OR_FOLLOWER` is reported as retriable.
- No malformed data is returned.
- Manual recovery with fresh metadata succeeds.

Automatic recovery must complete within the retry budget.

### 10. Broker pause and resume

Pause the broker process for less than, equal to, and greater than `requestTimeoutMs`.

Pass conditions:

- Short pauses can complete if the request timeout permits.
- Long pauses reject by timeout.
- Resume does not deliver one response to the wrong correlation ID.
- Late timed-out responses are ignored safely.

### 11. Broker disk full or read-only

Use a disposable broker volume. Fill it or make it read-only during Produce.

Pass conditions:

- Broker errors reach the caller.
- Failed sends do not remain queued forever.
- Memory stays bounded.
- The client does not report success for an error response.

### 12. Topic deletion and recreation

Delete an active topic, then recreate it with the same name.

Pass conditions:

- Requests fail clearly while metadata is invalid.
- No records from the old topic identity are treated as records from the recreated topic without an explicit restart or metadata refresh.
- The client does not loop at full CPU.

### 13. TLS failure

Test these faults:

- Untrusted certificate
- Wrong server name
- Expired certificate
- Server closes during handshake
- Client certificate rejected

Pass conditions:

- Connection fails before Kafka bytes are accepted.
- The error does not expose private key data.
- No plaintext fallback occurs.
- Repeated failures do not leak sockets or memory.

### 14. Malformed protocol response

Use the Bun mock broker to send:

- Negative frame size
- Frame larger than `maxResponseBytes`
- Unknown correlation ID
- Truncated fixed-width field
- Invalid array length
- Invalid varint or varlong
- Invalid record length
- CRC32C mismatch
- Unsupported compression bits

Pass conditions:

- The client rejects structurally malformed input.
- It ignores an unknown or late correlation ID safely.
- It does not allocate more than `maxResponseBytes` for one frame.
- It does not return partial messages.
- It does not crash Bun.

## Chaos release gates

### Gate A: failure-safe

Required before the native client is described as failure-safe:

- [ ] Bootstrap failover passes.
- [ ] Produce reset and timeout pass.
- [ ] Fetch reset and timeout pass.
- [ ] Partial-frame cleanup passes.
- [ ] Malformed protocol suite passes.
- [ ] Shutdown under failure passes.
- [ ] No memory or socket leak is found in 1,000 fault cycles.

### Gate B: application-assisted recovery

Required before recommendation for a production service that owns retries:

- [ ] Gate A passes.
- [ ] Data oracle documents retry duplicates.
- [ ] Manual leader refresh or client recreation is tested.
- [ ] Retry policy has a maximum attempt count and total deadline.
- [ ] Application message IDs make duplicate handling safe.

### Gate C: seamless client recovery

Not available in the current implementation. Version validation, connect timeout, metadata refresh, retry classification, backoff, and basic group offsets now exist. Gate C still requires:

- ApiVersions-based request version selection
- Total retry deadline
- Broker-failure qualification of idempotent producer state
- Reliable consumer-group rejoin during coordinator and membership changes

Do not mark Gate C complete until broker-kill, leader-transfer, rolling-restart, and network-blackhole tests pass without application client recreation.

## Chaos result report

For each scenario, record:

- Commit and environment
- Exact fault command
- Injection and recovery timestamps
- Active broker and leader IDs
- Requests started, completed, rejected, and timed out
- Produced and consumed message IDs
- Unknown Produce results
- Missing and duplicate counts
- Maximum queue, memory, CPU, socket, and file descriptor values
- Recovery time
- Pass or fail reason
