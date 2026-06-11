# TODO — pgmq-ruby gaps vs upstream

## 1. create_non_partitioned (minor)

`pgmq.create_non_partitioned(queue_name)` is an explicit alias for the default `create()`
behaviour. Provides symmetry with `create_partitioned` and `create_unlogged` for code
that creates queue types generically. Low priority — `create()` already does the same thing.

---

## Resolved / Dropped

- ✓ `list_notify_insert_throttles` — PR #128
- ✓ `update_notify_insert` — PR #127
- ✓ `read_grouped_head` — PR #121
- ✓ `create_fifo_index` / `create_fifo_indexes_all` — PR #123
- ✓ `convert_archive_partitioned` — PR #124
- ✗ `read_grouped_head_with_poll` — does not exist in PGMQ v1.11.1 (confirmed not in
  stable release or source). Dropped from watch list.
- ✗ `detach_archive` — exists upstream but is a deprecated no-op scheduled for removal
  in v2.0. Not worth wrapping.
