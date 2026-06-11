# TODO — pgmq-ruby gaps vs upstream

## 1. Missing notification management methods

Two functions from the NOTIFY family are not yet wrapped:

- `list_notify_insert_throttles()` — returns all queues that have a NOTIFY trigger,
  with their `throttle_interval_ms` and `last_notified_at`. Useful for inspecting
  notification configuration across all queues at once.

- ~~`update_notify_insert(queue_name, throttle_interval_ms)` — updates the throttle
  interval on an already-enabled NOTIFY trigger without having to disable/re-enable.~~ ✓ Done

Both were added in PGMQ v1.11.0. Existing neighbours in `lib/pgmq/client/maintenance.rb`:
- `enable_notify_insert` ✓
- `disable_notify_insert` ✓

## 2. create_non_partitioned (minor)

`pgmq.create_non_partitioned(queue_name)` is an explicit alias for the default `create()`
behaviour. Provides symmetry with `create_partitioned` and `create_unlogged` for code
that creates queue types generically. Low priority — `create()` already does the same thing.

---

## Resolved / Dropped

- ✓ `read_grouped_head` — PR #121
- ✓ `create_fifo_index` / `create_fifo_indexes_all` — PR #123
- ✓ `convert_archive_partitioned` — PR #124
- ✗ `read_grouped_head_with_poll` — does not exist in PGMQ v1.11.1 (confirmed not in
  stable release or source). Dropped from watch list.
- ✗ `detach_archive` — exists upstream but is a deprecated no-op scheduled for removal
  in v2.0. Not worth wrapping.
