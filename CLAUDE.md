# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PGMQ-Ruby is a **low-level Ruby client** for PGMQ (PostgreSQL Message Queue), analogous to how rdkafka-ruby relates to Kafka. It provides direct 1:1 wrappers for PGMQ SQL functions as a thin transport layer.

This is **NOT** a full job processing framework. Framework features (instrumentation, job processing, Rails ActiveJob, retry strategies, monitoring integrations) belong in the planned `pgmq-framework` gem.

**Key Design Principles:**
- Thin wrapper around PGMQ SQL functions (low-level primitives only)
- Thread-safe connection pooling (transport layer)
- PostgreSQL transaction support (database primitive, not framework abstraction)
- Framework-agnostic (works with Rails, Sinatra, plain Ruby)
- Minimal dependencies (only `pg` and `connection_pool`)
- No instrumentation/observability layer (framework concern)

## Development Commands

### Testing

```bash
# Start PostgreSQL with PGMQ extension (required for tests)
docker compose up -d

# Run all tests
bundle exec rake test

# Run specific test file
bundle exec ruby -Ilib:test test/lib/pgmq/client_test.rb

# Run single test by name
bundle exec ruby -Ilib:test test/lib/pgmq/client_test.rb -n /test_name/
```

**Important:** PostgreSQL runs on port **5433** locally (see docker-compose.yml) to avoid conflicts with system PostgreSQL. Tests use this port automatically.

### Code Quality

```bash
# Run tests
bundle exec rake test
```


### Interactive Development

```bash
# Start IRB console with PGMQ loaded
bundle exec bin/console

# Console automatically connects to test database at localhost:5433
```

## Architecture

### Core Components

1. **PGMQ::Client** (`lib/pgmq/client.rb`)
   - Main interface for all PGMQ operations (modular architecture, ~130 lines)
   - Delegates connection management to PGMQ::Connection
   - Validates queue names (48 char max, PostgreSQL identifier rules)
   - Composed of 8 functional modules for separation of concerns:
     - **Transaction** (`lib/pgmq/transaction.rb`) - PostgreSQL transaction support
     - **QueueManagement** (`lib/pgmq/client/queue_management.rb`) - Queue lifecycle (create, drop, list)
     - **Producer** (`lib/pgmq/client/producer.rb`) - Message sending operations
     - **Consumer** (`lib/pgmq/client/consumer.rb`) - Single-queue reading operations
     - **MultiQueue** (`lib/pgmq/client/multi_queue.rb`) - Multi-queue operations (UNION ALL)
     - **MessageLifecycle** (`lib/pgmq/client/message_lifecycle.rb`) - Message state transitions (pop, delete, archive, set_vt)
     - **Maintenance** (`lib/pgmq/client/maintenance.rb`) - Queue maintenance (purge, notifications)
     - **Metrics** (`lib/pgmq/client/metrics.rb`) - Monitoring and metrics
   - Benefits: Each module can be tested independently, easier maintenance, clear separation of concerns
   - Pattern inspired by Waterdrop's modular client architecture

2. **PGMQ::Connection** (`lib/pgmq/connection.rb`)
   - Thread-safe connection pooling using `connection_pool` gem
   - Supports multiple connection strategies:
     - Connection strings: `postgres://user:pass@host/db`
     - Hash parameters: `{ host:, port:, dbname:, user:, password: }`
     - Callables (for Rails): `-> { ActiveRecord::Base.connection.raw_connection }`
   - Auto-reconnect on connection failures (configurable)
   - Lost-connection matchers are extendable via `reconnectable_error_patterns:`
     (Strings/Regexps) and `reconnectable_error_classes:` keyword arguments,
     so users can adapt to new pg/PostgreSQL/pooler disconnect signatures
     without waiting for a gem release
   - Connection health checks before use
   - Note: Connection parameters are required - users should manage ENV variables themselves using `ENV.fetch`

3. **PGMQ::Transaction** (`lib/pgmq/transaction.rb`)
   - Low-level PostgreSQL transaction support (database primitive)
   - Wraps PostgreSQL's native transactions (NOT a framework abstraction)
   - Analogous to rdkafka-ruby providing Kafka transaction support
   - Mixin providing `client.transaction do |txn|` support
   - Enables atomic operations across multiple queues
   - Rolls back automatically on errors

4. **Models**
   - **PGMQ::Message** - Represents queue messages with `msg_id`, `message` (raw JSONB), `read_ct`, `enqueued_at`, `vt`, `queue_name` (for multi-queue ops)
   - **PGMQ::Metrics** - Queue metrics (length, age, total messages)
   - **PGMQ::QueueMetadata** - Queue information (name, creation time)

### Connection Pooling Details

- Default pool size: 5 connections
- Default timeout: 5 seconds
- Fiber-aware (works with Ruby 3.0+ Fiber Scheduler)
- Auto-reconnect enabled by default (can be disabled)
- Connection health verified before each use (if auto-reconnect enabled)

### Error Hierarchy

```
PGMQ::Errors::BaseError (StandardError)
├── PGMQ::Errors::ConnectionError
├── PGMQ::Errors::QueueNotFoundError
├── PGMQ::Errors::MessageNotFoundError
├── PGMQ::Errors::SerializationError
├── PGMQ::Errors::ConfigurationError
└── PGMQ::Errors::InvalidQueueNameError
```

### Queue Name Validation

- Maximum 48 characters (PGMQ limitation for table prefixes)
- Must start with letter or underscore
- Only letters, digits, underscores allowed
- Case-sensitive
- Validated in `Client#validate_queue_name!`

### Modular Architecture Pattern

The Client class follows Waterdrop's modular architecture pattern for better maintainability:

**Structure:**
- Small core `Client` class (~130 lines) that includes functional modules
- Each module focuses on a single domain area (queue management, producer, consumer, etc.)
- Modules access parent class helpers (`validate_queue_name!`, `with_connection`)
- Complete backward compatibility - no API changes

**Benefits:**
- **Testability**: Each module can be tested independently
- **Maintainability**: Smaller files, clearer boundaries (largest file ~210 lines)
- **Discoverability**: Logical grouping makes finding methods easier
- **Extensibility**: New modules can be added without bloating the core class

**Module Organization:**
1. Transaction support (existing mixin)
2. Queue lifecycle operations (create, drop, list)
3. Message production (sending)
4. Single-queue consumption (reading)
5. Multi-queue operations (efficient polling across queues)
6. Message lifecycle (pop, delete, archive, visibility timeout)
7. Maintenance operations (purge, detach archive)
8. Metrics and monitoring

All modules are automatically loaded via Zeitwerk.

## Common Patterns

### Client Initialization

```ruby
# Connection string (preferred)
client = PGMQ::Client.new('postgres://localhost:5433/pgmq_test')

# Connection hash
client = PGMQ::Client.new(
  host: 'localhost',
  port: 5433,
  dbname: 'pgmq_test',
  user: 'postgres',
  password: 'postgres'
)

# Connection hash using ENV variables (user manages ENV themselves)
client = PGMQ::Client.new(
  host: ENV.fetch('PG_HOST', 'localhost'),
  port: ENV.fetch('PG_PORT', 5432).to_i,
  dbname: ENV.fetch('PG_DATABASE', 'pgmq'),
  user: ENV.fetch('PG_USER', 'postgres'),
  password: ENV.fetch('PG_PASSWORD', 'postgres')
)

# Rails integration (reuses Rails connection pool)
client = PGMQ::Client.new(-> { ActiveRecord::Base.connection.raw_connection })

# Custom pool configuration
client = PGMQ::Client.new(
  'postgres://localhost/db',
  pool_size: 10,
  pool_timeout: 10,
  auto_reconnect: false
)
```

### Transaction Pattern

```ruby
# Atomic operations across queues
client.transaction do |txn|
  msg = txn.read('pending', vt: 30)
  if msg
    txn.produce('processed', msg.payload)
    txn.delete('pending', msg.msg_id)
  end
end
```

### Long Polling Pattern

```ruby
# Efficient message consumption
loop do
  msg = client.read_with_poll('orders',
    vt: 30,
    max_poll_seconds: 5,
    poll_interval_ms: 100
  )
  break unless msg

  process(msg)
  client.delete('orders', msg.msg_id)
end
```

## Testing Guidelines

### Test Structure

- **Unit tests** (`test/lib/`) - Minitest/spec tests for classes in isolation and with database
- **Integration tests** (`spec/integration/`) - Standalone example scripts with real PostgreSQL

### Test Helpers

Located in `test/support/database_helpers.rb`:
- Database cleanup between tests
- Connection helpers
- Common test utilities

### Coverage Requirements

- Minimum overall coverage: 96.5%
- SimpleCov configured in `test/test_helper.rb`

### Writing Tests

- Use descriptive test names
- Follow AAA pattern (Arrange, Act, Assert)
- Mock external dependencies in unit tests
- Use real database for integration tests
- Clean up queues in `after` blocks

## Code Style

- Ruby 3.3+ syntax
- Frozen string literals (`# frozen_string_literal: true`)
- Max line length: 120 characters (except tests/gemspec)
- YARD documentation for public methods
- Single quotes for strings (unless interpolation needed)

## CI/CD

GitHub Actions workflows in `.github/workflows/`:
- **ci.yml** - Runs minitest on all Ruby versions (3.3, 3.4, 4.0)
- **push.yml** - Additional checks on push

## Dependencies

**Runtime:**
- `pg` (~> 1.5) - PostgreSQL adapter
- `connection_pool` (~> 2.4) - Thread-safe connection pooling

**Development:**
- `minitest` - Testing framework
- `mocha` - Mocking library
- `simplecov` - Code coverage


## Version Information

- Minimum Ruby: 3.3.0
- Supported PostgreSQL: 14-18 with PGMQ extension
- License: LGPL-3.0

## Important Files

### Core Library Structure
- `lib/pgmq/client.rb` - Main public API (~130 lines, includes all modules)
- `lib/pgmq/client/` - Client functional modules directory:
  - `queue_management.rb` - Queue lifecycle operations (~100 lines)
  - `producer.rb` - Message sending (~70 lines)
  - `consumer.rb` - Single-queue reading (~140 lines)
  - `multi_queue.rb` - Multi-queue operations (~200 lines)
  - `message_lifecycle.rb` - Message state transitions (~210 lines)
  - `maintenance.rb` - Queue maintenance (~30 lines)
  - `metrics.rb` - Monitoring (~40 lines)
- `lib/pgmq/connection.rb` - Connection pooling and management
- `lib/pgmq/transaction.rb` - Transaction support
- `lib/pgmq/message.rb` - Message model (Data class)
- `lib/pgmq/metrics.rb` - Metrics model
- `lib/pgmq/queue_metadata.rb` - Queue metadata model

### Documentation & Testing
- `test/test_helper.rb` - Minitest configuration + SimpleCov setup
- `test/lib/` - Minitest/spec unit tests
- `spec/integration/` - Standalone integration examples with real PostgreSQL
- `DEVELOPMENT.md` - Comprehensive development documentation
- `README.md` - User-facing documentation with all API examples
- `CLAUDE.md` - AI assistant guidance (this file)
