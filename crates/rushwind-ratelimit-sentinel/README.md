# rushwind-ratelimit-sentinel — deferred

The Go `sentinel` engine is an adapter over `sentinel-golang` (Alibaba's
flow-control SDK): it wraps the Entry/Exit lifecycle for a named resource
whose rules are configured out-of-band via `flow.LoadRules`.

There is no Rust port of sentinel-golang, so there is nothing to adapt —
the engine would have to reimplement Sentinel's entire flow-control core
(token calculation, reject/throttle behaviors, warm-up, statistic sliding
windows), which is a standalone project, not an adapter.

If a Rust Sentinel-compatible limiter is ever needed, prefer porting the
flow-control semantics on top of `rushwind-ratelimit-tokenbucket` (direct
threshold + reject maps 1:1 onto a token bucket with burst = threshold).
