# Design notes and tradeoffs

## Goals
Keep one stable endpoint for clients while upstreams fail, slow down or hang. A failure should be invisible to subscription clients when possible. When it can't be hidden, the client should get an explicit error rather than silence.

## Components
- **`pool`**: endpoint state, plus probes and ranking. Probes run concurrently, and one slow endpoint can't delay the others' updates. Ranking is EWMA latency (α=0.3). An endpoint with no latency sample ranks after every measured one, and ties keep configuration order, so priority can still be expressed through config order.
- **`breaker`**: counts consecutive failures. Half-open admits exactly one trial, so a recovering upstream isn't flooded. The breaker is fed by probes *and* real traffic, so a backend that passes probes but fails real calls still gets cut off.
- **`supervisor`**: restarts with jittered, capped exponential backoff, and the backoff resets after a long healthy run. Panics are recovered and reported as errors. The goal was explicit restart and log semantics instead of swallowing errors with `except Exception: pass`.
- **`proxy`**: HTTP failover plus WebSocket sessions.

## Key decisions
1. **Each WebSocket session runs on one goroutine that owns all its state.** Client and upstream readers only push frames into channels, so the subscription maps need no locks and the race detector stays quiet. The cost is that one slow client write delays that session's upstream processing. That's acceptable, because the backpressure stays inside the slow client's own session.
2. **Clients keep their original subscription ID.** After failover, notifications are rewritten to the ID the client knows. This keeps clients unchanged. The cost is one JSON decode and encode per notification on rewritten subscriptions, measured at about 8 µs and 58 allocs. A faster version would splice bytes at the known offset of `"subscription"`.
3. **A new upstream can reuse an ID the client already holds** (for example `0x1` from two different providers). The proxy then issues its own ID (`0xrfp…`) for the new subscription.
4. **An upstream that accepts a WebSocket is not trusted yet.** Success is reported to the breaker only after the upstream sends its first frame. The session also avoids the upstream that just failed it, if any other is available. Without this, a hung upstream that still passes HTTP probes would be picked again forever. The chaos test for silent hangs caught exactly that bug.
5. **In-flight requests are not replayed.** After failover, unanswered requests get a JSON-RPC error saying the outcome is unknown. Replaying a write, such as a transaction submission, could do it twice. Only the client knows whether a retry is safe. Pending subscribe requests are failed the same way.
6. **HTTP retries are safe only for idempotent calls.** A retryable status (429, 502, 503, 504) or a connection error moves the request to the next upstream. For non-idempotent methods, a 502 from a gateway that did forward the request could cause a duplicate. Deployments with such methods should set `MaxAttempts=1` or route those methods separately. Per-method retry policy is future work.
7. **Responses are buffered, not streamed.** That allows failover when a body breaks mid-read and caps response size. The cost is memory per in-flight request (10 MiB maximum) and no streaming for very large responses.
8. **Liveness detection uses ping/pong with a read deadline of 2× the ping interval.** Any upstream frame also extends the deadline. A lower interval detects hangs faster but adds traffic. The default is 5 s, so a silent hang is detected in about 10 s. The chaos test uses 50 ms.

## Not done (yet)
- Hedged requests (send to two upstreams, take the first answer) for tail-latency-sensitive reads.
- Per-method routing and retry policy, and response caching for immutable calls.
- A block-height or freshness check that ranks out upstreams that are healthy but lagging (needed for blockchain RPC).
- A Helm chart. Plain manifests are provided.
- Load tests over a real network. The measured numbers are loopback only.
