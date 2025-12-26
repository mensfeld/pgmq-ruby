# Changelog

## 0.4.0 (2025-12-26)

### Breaking Changes
- **[Breaking]** Rename `send` to `produce` and `send_batch` to `produce_batch`. This avoids shadowing Ruby's built-in `Object#send` method which caused confusion and required workarounds (e.g., using `__send__`). The new names also align better with the producer/consumer terminology used in message queue systems.

### Queue Management
- [Enhancement] `create`, `create_partitioned`, and `create_unlogged` now return `true` if the queue was newly created, `false` if it already existed. This provides clearer feedback and aligns with the Rust PGMQ client behavior.

### Message Operations
- **[Feature]** Add `headers:` parameter to `produce(queue_name, message, headers:, delay:)` for message metadata (routing, tracing, correlation IDs).
- **[Feature]** Add `headers:` parameter to `produce_batch(queue_name, messages, headers:, delay:)` for batch message metadata.
- **[Feature]** Introduce `pop_batch(queue_name, qty)` for atomic batch pop (read + delete) operations.
- **[Feature]** Introduce `set_vt_batch(queue_name, msg_ids, vt_offset:)` for batch visibility timeout updates.
- **[Feature]** Introduce `set_vt_multi(updates_hash, vt_offset:)` for updating visibility timeouts across multiple queues atomically.

### Notifications
- **[Feature]** Introduce `enable_notify_insert(queue_name, throttle_interval_ms:)` for PostgreSQL LISTEN/NOTIFY support.
- **[Feature]** Introduce `disable_notify_insert(queue_name)` to disable notifications.

### Compatibility
- [Enhancement] Add Ruby 4.0.0 support with full CI testing.

## 0.3.0 (2025-11-14)

Initial release of pgmq-ruby - a low-level Ruby client for PGMQ (PostgreSQL Message Queue).

**Architecture Philosophy**: This library follows the rdkafka-ruby/Karafka pattern, providing a thin transport layer with direct 1:1 wrappers for PGMQ SQL functions. High-level features (instrumentation, job processing, Rails ActiveJob, retry strategies) belong in the planned `pgmq-framework` gem.

### Queue Management
- **[Feature]** Introduce `create(queue_name)` for standard queue creation.
- **[Feature]** Introduce `create_partitioned(queue_name, partition_interval:, retention_interval:)` for partitioned queues.
- **[Feature]** Introduce `create_unlogged(queue_name)` for high-throughput unlogged queues.
- **[Feature]** Introduce `drop_queue(queue_name)` for queue removal.
- **[Feature]** Introduce `list_queues` to retrieve all queue metadata.
- **[Feature]** Introduce `detach_archive(queue_name)` for archive table detachment.
- [Enhancement] Add queue name validation (48-char max, PostgreSQL identifier rules).

### Message Operations
- **[Feature]** Introduce `send(queue_name, message, delay:)` for single message publishing.
- **[Feature]** Introduce `send_batch(queue_name, messages)` for batch publishing.
- **[Feature]** Introduce `read(queue_name, vt:)` for single message consumption.
- **[Feature]** Introduce `read_batch(queue_name, vt:, qty:)` for batch consumption.
- **[Feature]** Introduce `read_with_poll(queue_name, vt:, max_poll_seconds:, poll_interval_ms:)` for long-polling.
- **[Feature]** Introduce `pop(queue_name)` for atomic read+delete operations.
- **[Feature]** Introduce `delete(queue_name, msg_id)` and `delete_batch(queue_name, msg_ids)` for message deletion.
- **[Feature]** Introduce `archive(queue_name, msg_id)` and `archive_batch(queue_name, msg_ids)` for message archival.
- **[Feature]** Introduce `purge_queue(queue_name)` for removing all messages.
- **[Feature]** Introduce `set_vt(queue_name, msg_id, vt:)` for visibility timeout updates.

### Multi-Queue Operations
- **[Feature]** Introduce `read_multi(queue_names, vt:, qty:, limit:)` for reading from multiple queues in single SQL query.
- **[Feature]** Introduce `read_multi_with_poll(queue_names, vt:, max_poll_seconds:, poll_interval_ms:)` for long-polling multiple queues.
- **[Feature]** Introduce `pop_multi(queue_names)` for atomic read+delete from first available queue.
- **[Feature]** Introduce `delete_multi(queue_to_msg_ids_hash)` for transactional bulk delete across queues.
- **[Feature]** Introduce `archive_multi(queue_to_msg_ids_hash)` for transactional bulk archive across queues.
- [Enhancement] All multi-queue operations use single database connection for efficiency.
- [Enhancement] Support for up to 50 queues per operation with automatic validation.
- [Enhancement] Queue name returned with each message for source tracking.

### Conditional JSONB Filtering
- **[Feature]** Introduce server-side message filtering using PostgreSQL's JSONB containment operator (`@>`).
- **[Feature]** Add `conditional:` parameter to `read()`, `read_batch()`, and `read_with_poll()` methods.
- [Enhancement] Support for filtering by single or multiple conditions with AND logic.
- [Enhancement] Support for nested JSON property filtering.
- [Enhancement] Filtering happens in PostgreSQL before returning results.

### Transaction Support
- **[Feature]** Introduce PostgreSQL transaction support via `client.transaction do |txn|` block.
- [Enhancement] Automatic rollback on errors for queue operations.
- [Enhancement] Enable atomic multi-queue coordination and exactly-once processing patterns.

### Monitoring & Metrics
- **[Feature]** Introduce `metrics(queue_name)` for queue statistics.
- **[Feature]** Introduce `metrics_all` for all queue statistics.
- **[Feature]** Introduce `stats()` for connection pool statistics.

### Connection Management
- **[Feature]** Introduce thread-safe connection pooling using `connection_pool` gem.
- **[Feature]** Support multiple connection strategies (string, hash, ENV variables, callables, direct injection).
- **[Feature]** Add configurable pool size (default: 5) and timeout (default: 5 seconds).
- **[Feature]** Add `auto_reconnect` option (default: true) for automatic connection recovery.
- [Enhancement] Connection health checks before use.
- [Enhancement] Full Fiber scheduler compatibility (Ruby 3.0+).
- [Enhancement] Proper handling of stale/closed connections with automatic reset.
- [Enhancement] Clear error messages for connection pool timeout.

### Rails Integration
- [Enhancement] Seamless ActiveRecord connection reuse via proc/lambda.
- [Enhancement] Zero additional database connections when using Rails connection pool.

### Serialization
- **[Feature]** Introduce pluggable serializer system.
- **[Feature]** Provide JSON serializer (default).
- **[Feature]** Support custom serializers via `PGMQ::Serializers::Base`.

### Error Handling
- **[Feature]** Introduce comprehensive error hierarchy with specific error classes for different failure modes.

### Value Objects
- **[Feature]** Introduce `PGMQ::Message` with msg_id, read_ct, enqueued_at, vt, payload.
- **[Feature]** Introduce `PGMQ::Metrics` for queue statistics.
- **[Feature]** Introduce `PGMQ::QueueMetadata` for queue information.

### Testing & Quality
- [Enhancement] 97.19% code coverage with 201 tests.
- [Enhancement] Comprehensive integration and unit test suites.
- [Enhancement] GitHub Actions CI/CD with matrix testing (Ruby 3.2-3.5, PostgreSQL 14-18).
- [Enhancement] Docker Compose setup for development.

### Security
- [Enhancement] Parameterized queries for all SQL operations preventing SQL injection.

### Documentation
- [Enhancement] Comprehensive README with quick start, API reference, and examples.
- [Enhancement] DEVELOPMENT.md for contributors.
- [Enhancement] Example scripts demonstrating all features.

### Dependencies
- Ruby >= 3.2.0
- PostgreSQL >= 14 with PGMQ extension
- `pg` gem (~> 1.5)
- `connection_pool` gem (~> 2.4)
- `zeitwerk` gem (~> 2.6)
