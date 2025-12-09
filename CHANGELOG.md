# Changelog

All notable changes to Zyn will be documented in this file.

## main / unreleased

* [CHANGE]: Expand correlation IDs from `u16` to `u32` across protocol. [#60](https://github.com/zyn-org/zyn/pull/60)
* [ENHANCEMENT]: Cache allowed broadcast targets to reduce lock contention during message broadcasting. [#68](https://github.com/zyn-org/zyn/pull/68)
* [ENHANCEMENT]: Replace manual sharding with DashMap in c2s router. [#69](https://github.com/zyn-org/zyn/pull/69)
* [ENHANCEMENT]: Move high-frequency operation logs from info to trace level. [#72](https://github.com/zyn-org/zyn/pull/72)
* [ENHANCEMENT]: Pin cancellation token futures in select loops to reduce lock contention. [#75](https://github.com/zyn-org/zyn/pull/75)
* [ENHANCEMENT]: Migrate buffer pool to lock-free async implementation. [#78](https://github.com/zyn-org/zyn/pull/78)
* [ENHANCEMENT]: Improve bucketed pool acquire to wait on random bucket when exhausted. [#79](https://github.com/zyn-org/zyn/pull/79)
* [BUGFIX]: Ensure the connection is always gracefully shut down. [#65](https://github.com/zyn-org/zyn/pull/65)
* [BUGFIX]: Only leave channels when user's last connection closes. [#70](https://github.com/zyn-org/zyn/pull/70)

## 0.2.0 (2024-07-26)

* [CHANGE]: Optimize writes with vectored I/O implementation. [#39](https://github.com/zyn-org/zyn/pull/39)
* [ENHANCEMENT]: Dynamic fd limits based on config. [#37](https://github.com/zyn-org/zyn/pull/37)
* [ENHANCEMENT]: Add flush batching support for improved connection throughput. [#45](https://github.com/zyn-org/zyn/pull/45)
* [BUGFIX]: Correct rate limit checking in connection handling. [#35](https://github.com/zyn-org/zyn/pull/35)
