# TODO — PGMQ v1.11.1 gaps

## 1. Missing FIFO read variants

`read_grouped_rr` / `read_grouped_rr_with_poll` are present, but two strategies are missing:

- `read_grouped(queue_name, vt:, qty:)` — SQS-style, drains oldest group first for max throughput
- `read_grouped_with_poll(queue_name, vt:, qty:, max_poll_seconds:, poll_interval_ms:)` — poll variant of the above
- `read_grouped_head(queue_name, vt:, qty:)` — one message per group, added in v1.11.1

`read_grouped` and `read_grouped_rr` have different semantics: `rr` interleaves groups fairly,
`read_grouped` maximises throughput by draining the oldest group first.

## 2. Missing FIFO index management

No wrappers for:

- `create_fifo_index(queue_name)` — creates the index required for grouped reads on one queue
- `create_fifo_indexes_all()` — creates it for all queues

Users who create queues and then use grouped reads will hit performance issues without these.

## 3. Missing `convert_archive_partitioned`

`pgmq.convert_archive_partitioned(table_name, partition_interval, retention_interval, leading_partition)`
converts an existing archive table to pg_partman-managed partitions.
`create_partitioned` exists for new queues but there is no migration path for existing ones.

## 4. Missing `detach_archive`

`maintenance.rb` even mentions it in its opening comment but there is no implementation.
Detaches the archive table from PGMQ management without dropping it.

---

## Priority order

1. `read_grouped` + `read_grouped_with_poll` — mirrors existing rr methods, exposes both upstream strategies
2. `read_grouped_head` — simple wrapper, newest upstream addition (v1.11.1)
3. FIFO index management (`create_fifo_index`, `create_fifo_indexes_all`) — needed for correctness at scale
4. `detach_archive` + `convert_archive_partitioned` — maintenance/migration helpers, lower urgency
