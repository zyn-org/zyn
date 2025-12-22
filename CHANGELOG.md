# Changelog

All notable changes to Narwhal will be documented in this file.

## main / unreleased

* [CHANGE]: Support alphanumeric channel identifiers. [#105](https://github.com/narwhal-io/narwhal/pull/105)
* [CHANGE]: Add `ResourceConflict` error for concurrent modification scenarios. [#109](https://github.com/narwhal-io/narwhal/pull/109)
* [ENHANCEMENT]: Add pagination support to `CHANNELS`/`CHANNELS_ACK` messages. [#110](https://github.com/narwhal-io/narwhal/pull/110), [#112](https://github.com/narwhal-io/narwhal/pull/112)
* [ENHANCEMENT]: Add pagination support to `MEMBERS`/`MEMBERS_ACK` messages. [#113](https://github.com/narwhal-io/narwhal/pull/113)
* [ENHANCEMENT]: Reduce lock contention in channel join and configuration operations. [#108](https://github.com/narwhal-io/narwhal/pull/108)

## 0.3.0 (2025-12-12) 🎄

* [CHANGE]: Expand correlation IDs from `u16` to `u32` across protocol. [#60](https://github.com/narwhal-io/narwhal/pull/60)
* [ENHANCEMENT]: Cache allowed broadcast targets to reduce lock contention during message broadcasting. [#68](https://github.com/narwhal-io/narwhal/pull/68)
* [ENHANCEMENT]: Replace manual sharding with DashMap in c2s router. [#69](https://github.com/narwhal-io/narwhal/pull/69)
* [ENHANCEMENT]: Move high-frequency operation logs from info to trace level. [#72](https://github.com/narwhal-io/narwhal/pull/72)
* [ENHANCEMENT]: Pin cancellation token futures in select loops to reduce lock contention. [#75](https://github.com/narwhal-io/narwhal/pull/75)
* [ENHANCEMENT]: Migrate buffer pool to lock-free async implementation. [#78](https://github.com/narwhal-io/narwhal/pull/78)
* [ENHANCEMENT]: Simplify keep-alive with activity-based ping loop to eliminate race conditions. [#82](https://github.com/narwhal-io/narwhal/pull/82)
* [ENHANCEMENT]: Refactor write batch handling for efficient pool buffer release. [#85](https://github.com/narwhal-io/narwhal/pull/85)
* [ENHANCEMENT]: Improve bucketed pool acquire to block on smallest suitable bucket when buffers unavailable. [#86](https://github.com/narwhal-io/narwhal/pull/86)
* [BUGFIX]: Ensure the connection is always gracefully shut down. [#65](https://github.com/narwhal-io/narwhal/pull/65)
* [BUGFIX]: Only leave channels when user's last connection closes. [#70](https://github.com/narwhal-io/narwhal/pull/70)

## 0.2.0 (2025-07-26)

* [CHANGE]: Optimize writes with vectored I/O implementation. [#39](https://github.com/narwhal-io/narwhal/pull/39)
* [ENHANCEMENT]: Dynamic fd limits based on config. [#37](https://github.com/narwhal-io/narwhal/pull/37)
* [ENHANCEMENT]: Add flush batching support for improved connection throughput. [#45](https://github.com/narwhal-io/narwhal/pull/45)
* [BUGFIX]: Correct rate limit checking in connection handling. [#35](https://github.com/narwhal-io/narwhal/pull/35)
