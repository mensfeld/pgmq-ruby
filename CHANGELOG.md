# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2025-01-XX (Unreleased)

### About This Release

PGMQ-Ruby is a **low-level Ruby client** for [PGMQ](https://github.com/pgmq/pgmq) (PostgreSQL Message Queue). This library follows the rdkafka-ruby/Karafka architectural pattern - providing direct access to all PGMQ operations as a thin transport layer.

> **Architecture Note**: This is the low-level foundation library, analogous to rdkafka-ruby. High-level features (instrumentation, job processing, Rails ActiveJob integration, retry strategies, monitoring integrations) belong in `pgmq-framework` (similar to how Karafka builds on rdkafka-ruby).

**What This Library Provides:**
- Direct 1:1 wrappers for PGMQ SQL functions
- Connection pooling and management (transport layer)
- PostgreSQL transaction support (database primitive)
- Pluggable serialization
- Thread-safe operations

**What This Library Does NOT Provide** (framework features for pgmq-framework):
- Instrumentation/observability systems
- Job processing frameworks
- Rails integration (generators, Railtie, ActiveJob)
- Retry strategies or error handling patterns
- Monitoring integrations (AppSignal, StatsD, DataDog)
- Web UI or management tools

### Core Features

#### Queue Management
- **[Feature]** Create standard queues with `create(queue_name)`
- **[Feature]** Create partitioned queues with `create_partitioned(queue_name, partition_interval:, retention_interval:)`
- **[Feature]** Create unlogged queues with `create_unlogged(queue_name)` for high-throughput scenarios
- **[Feature]** Drop queues with `drop_queue(queue_name)`
- **[Feature]** List all queues with `list_queues`
- **[Feature]** Enhanced queue name validation (48-char max, PostgreSQL identifier rules)
- **[Feature]** Detach archive tables with `detach_archive(queue_name)`

#### Message Operations
- **[Feature]** Send single messages with `send(queue_name, message, delay: seconds)`
- **[Feature]** Send batch messages with `send_batch(queue_name, messages)`
- **[Feature]** Read single message with `read(queue_name, vt: seconds)`
- **[Feature]** Read batch messages with `read_batch(queue_name, vt: seconds, qty: count)`
- **[Feature]** Long-polling with `read_with_poll(queue_name, vt:, max_poll_seconds:, poll_interval_ms:)`
- **[Feature]** Pop (atomic read+delete) with `pop(queue_name)`
- **[Feature]** Delete messages with `delete(queue_name, msg_id)` and `delete_batch(queue_name, msg_ids)`
- **[Feature]** Archive messages with `archive(queue_name, msg_id)` and `archive_batch(queue_name, msg_ids)`
- **[Feature]** Purge all messages with `purge_queue(queue_name)`
- **[Feature]** Set visibility timeout with `set_vt(queue_name, msg_id, vt: seconds)`

#### Multi-Queue Operations (Single Connection)
- **[Feature]** `read_multi(queue_names, vt:, qty:, limit:)` - Read from multiple queues in single SQL query
- **[Feature]** `read_multi_with_poll(queue_names, vt:, max_poll_seconds:, poll_interval_ms:)` - Long-poll multiple queues
- **[Feature]** `pop_multi(queue_names)` - Atomic read+delete from first available queue
- **[Feature]** `delete_multi(queue_to_msg_ids_hash)` - Transactional bulk delete across queues
- **[Feature]** `archive_multi(queue_to_msg_ids_hash)` - Transactional bulk archive across queues
- **[Enhancement]** All multi-queue operations use single database connection (efficient for high-throughput)
- **[Enhancement]** Support for up to 50 queues per operation with automatic validation
- **[Enhancement]** Queue name returned with each message for tracking source queue

#### Conditional JSONB Filtering
- **[Feature]** Server-side message filtering using PostgreSQL's JSONB containment operator (`@>`)
- **[Feature]** `conditional:` parameter for `read()`, `read_batch()`, and `read_with_poll()` methods
- **[Feature]** Support for filtering by single or multiple conditions with AND logic
- **[Feature]** Support for filtering by nested JSON properties
- **[Enhancement]** Filtering happens entirely in PostgreSQL - only matching messages are returned
- **[Enhancement]** `qty` parameter applies after filtering for efficient batch reads

#### Transaction Support
- **[Feature]** Transaction support via `client.transaction do |txn|` block
- **[Feature]** Automatic rollback on errors - any exception rolls back all queue operations
- **[Feature]** Atomic multi-queue coordination (send to multiple queues atomically)
- **[Feature]** Exactly-once processing patterns (atomic read + application update + delete)
- **[Feature]** Atomic message routing between queues without data loss

#### Monitoring & Metrics
- **[Feature]** Queue metrics with `metrics(queue_name)`
- **[Feature]** All queue metrics with `metrics_all`
- **[Feature]** Connection pool statistics with `stats()` method

#### Connection Management
- **[Feature]** Thread-safe connection pooling using `connection_pool` gem
- **[Feature]** Multiple connection strategies:
  - Connection string: `postgres://user:pass@host:port/db`
  - Connection hash: `{ host:, port:, dbname:, user:, password: }`
  - Environment variables: `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`
  - Callable providers: `-> { ActiveRecord::Base.connection.raw_connection }`
  - Direct PG::Connection injection
- **[Feature]** Configurable pool size (default: 5) and timeout (default: 5 seconds)
- **[Feature]** `auto_reconnect` option (default: true) - Automatically recover from lost connections
- **[Feature]** Connection health checks - Verify connections are alive before use
- **[Feature]** Full Fiber scheduler compatibility (Ruby 3.0+)
- **[Enhancement]** Proper handling of stale/closed connections with automatic reset
- **[Enhancement]** Clear error messages for connection pool timeout

#### Rails Integration
- **[Feature]** Seamless ActiveRecord connection reuse via proc/lambda
- **[Feature]** Zero additional database connections when using Rails connection pool
- **[Feature]** Full support for Rails connection checkout/checkin patterns

#### Serialization
- **[Feature]** Pluggable serializer system
- **[Feature]** JSON serializer (default)
- **[Feature]** MessagePack serializer for better performance (requires `msgpack` gem)
- **[Feature]** Custom serializer support via `PGMQ::Serializers::Base`

#### Error Handling
- **[Feature]** Comprehensive error hierarchy:
  - `PGMQ::Error` - Base error class
  - `PGMQ::ConnectionError` - Connection and pool errors
  - `PGMQ::InvalidQueueNameError` - Queue name validation errors
  - `PGMQ::ConfigurationError` - Configuration errors
  - `PGMQ::MessageNotFoundError` - Message not found errors
  - `PGMQ::QueueNotFoundError` - Queue not found errors
  - `PGMQ::SerializationError` - Serialization/deserialization errors

#### Value Objects
- **[Feature]** `PGMQ::Message` - Message with msg_id, read_ct, enqueued_at, vt, payload
- **[Feature]** `PGMQ::Metrics` - Queue metrics (length, newest/oldest age, scrape time, total messages)
- **[Feature]** `PGMQ::QueueMetadata` - Queue information (name, created_at, is_partitioned, is_unlogged)

### Quality & Testing

- **[Enhancement]** 201 tests, 0 failures
- **[Enhancement]** 97.19% code coverage
- **[Enhancement]** Comprehensive integration tests for all features
- **[Enhancement]** Unit tests for edge cases and error handling
- **[Enhancement]** GitHub Actions CI/CD with matrix testing (Ruby 3.2/3.3/3.4/3.5, PostgreSQL 14-18)
- **[Enhancement]** Docker Compose setup for development and testing

### Documentation

- **[Enhancement]** Comprehensive README with:
  - Quick start guide
  - Complete API reference
  - Multi-queue operation examples
  - Connection pooling guide
  - Rails integration examples
  - Performance tips
- **[Enhancement]** Development guide (DEVELOPMENT.md)
- **[Enhancement]** Example scripts demonstrating all features
- **[Enhancement]** CLAUDE.md for Claude Code guidance

### Dependencies

- Ruby >= 3.2.0
- PostgreSQL >= 14 with PGMQ extension
- `pg` gem (~> 1.5)
- `connection_pool` gem (~> 2.4)
- Optional: `msgpack` gem for MessagePack serializer

### Security

- **[Enhancement]** Proper parameterized queries for all SQL operations
- **[Enhancement]** No SQL injection vulnerabilities
- **[Enhancement]** Safe handling of user-provided queue names and message data

---

## Notes

This is the initial public release of pgmq-ruby as a low-level client library.

### Future Roadmap

High-level features will be available in `pgmq-framework` gem:
- ActiveJob adapter
- Rails generators and Railtie
- Job processor with signal handling
- Retry strategies (exponential backoff, jitter)
- Rake tasks for queue management
- Worker process management

[0.3.0]: https://github.com/mensfeld/pgmq-ruby/releases/tag/v0.3.0
