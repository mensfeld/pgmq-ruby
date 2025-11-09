# PGMQ-Ruby

[![Gem Version](https://badge.fury.io/rb/pgmq-ruby.svg)](https://badge.fury.io/rb/pgmq-ruby)
[![Build Status](https://github.com/mensfeld/pgmq-ruby/workflows/ci/badge.svg)](https://github.com/mensfeld/pgmq-ruby/actions)

**Low-level Ruby client for [PGMQ](https://github.com/pgmq/pgmq) - PostgreSQL Message Queue**

## What is PGMQ-Ruby?

PGMQ-Ruby is a **low-level Ruby client** for PGMQ (PostgreSQL Message Queue). It provides direct access to all PGMQ operations with a clean, minimal API - similar to how [rdkafka-ruby](https://github.com/karafka/rdkafka-ruby) relates to Kafka.

**Think of it as:**

- **Like AWS SQS** - but running entirely in PostgreSQL with no external dependencies
- **Like Sidekiq/Resque** - but without Redis, using PostgreSQL for both data and queues
- **Like rdkafka-ruby** - a thin, efficient wrapper around the underlying system (PGMQ SQL functions)

## When to Use PGMQ-Ruby?

**Use this library directly if you:**

- Need low-level control over queue operations
- Want to build custom job processing systems
- Are integrating PGMQ into existing applications
- Already use PostgreSQL and want to avoid additional infrastructure

**Use a higher-level library if you:**

- Need Rails ActiveJob integration → Use `pgmq-framework` (coming soon)
- Want automatic retries and error handling → Use `pgmq-framework` (coming soon)
- Need worker process management → Use `pgmq-framework` (coming soon)

> **Architecture Note**: This library follows the rdkafka-ruby/Karafka pattern - `pgmq-ruby` is the low-level foundation, while higher-level features (job processing, Rails integration, retry strategies) will live in `pgmq-framework` (similar to how Karafka builds on rdkafka-ruby).

## Key Features

- **100% PGMQ Feature Complete** - All SQL functions wrapped with idiomatic Ruby APIs
- **PostgreSQL Native** - No Redis, RabbitMQ, or external message brokers required
- **Thread-Safe** - Connection pooling with auto-reconnect and health checks
- **Multi-Queue Operations** - Read/pop/delete/archive from multiple queues in single queries
- **Framework Agnostic** - Works with Rails, Sinatra, Hanami, or plain Ruby
- **Rails-Friendly** - Easy integration with ActiveRecord connection pools
- **Minimal Dependencies** - Only `pg` and `connection_pool` gems required
- **High Test Coverage** - 200 tests, 93%+ coverage

## Table of Contents

- [Features](#features)
- [PGMQ Feature Support](#pgmq-feature-support)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Message Object](#message-object)
- [Serializers](#serializers)
- [Rails Integration](#rails-integration)
- [Testing](#testing)
- [Performance](#performance)
- [Comparison with Other Solutions](#comparison-with-other-solutions)
- [Development](#development)
- [Contributing](#contributing)
- [Resources](#resources)
- [License](#license)
- [Author](#author)

## Features

- **Postgres-native**: No external dependencies, runs entirely in PostgreSQL
- **Simple API**: Easy-to-use Ruby interface for queue operations
- **Rails-ready**: ActiveJob adapter and Rails integration **coming in v1.0**
- **Thread-safe**: Built-in connection pooling for concurrent use
- **Pluggable serializers**: JSON (default), custom serializers supported
- **Exactly-once delivery**: Within configurable visibility timeout windows
- **Battle-tested**: Based on proven patterns from Python, Rust, and JS clients

## PGMQ Feature Support

This gem provides complete support for all core PGMQ SQL functions. Based on the [official PGMQ API](https://tembo-io.github.io/pgmq/api/sql/functions/):

### Sending Messages
- [x] [send](https://tembo-io.github.io/pgmq/api/sql/functions/#send) - Send single message with optional delay
- [x] [send_batch](https://tembo-io.github.io/pgmq/api/sql/functions/#send_batch) - Send multiple messages atomically

### Reading Messages
- [x] [read](https://tembo-io.github.io/pgmq/api/sql/functions/#read) - Read single message with visibility timeout
- [x] [read_batch](https://tembo-io.github.io/pgmq/api/sql/functions/#read) - Read multiple messages with visibility timeout
- [x] [read_with_poll](https://tembo-io.github.io/pgmq/api/sql/functions/#read_with_poll) - Long-polling for efficient message consumption
- [x] [pop](https://tembo-io.github.io/pgmq/api/sql/functions/#pop) - Atomic read + delete operation

### Deleting/Archiving Messages
- [x] [delete](https://tembo-io.github.io/pgmq/api/sql/functions/#delete-single) - Delete single message
- [x] [delete_batch](https://tembo-io.github.io/pgmq/api/sql/functions/#delete-batch) - Delete multiple messages
- [x] [archive](https://tembo-io.github.io/pgmq/api/sql/functions/#archive-single) - Archive single message for long-term storage
- [x] [archive_batch](https://tembo-io.github.io/pgmq/api/sql/functions/#archive-batch) - Archive multiple messages
- [x] [purge_queue](https://tembo-io.github.io/pgmq/api/sql/functions/#purge_queue) - Remove all messages from queue

### Queue Management
- [x] [create](https://tembo-io.github.io/pgmq/api/sql/functions/#create) - Create standard queue
- [x] [create_partitioned](https://tembo-io.github.io/pgmq/api/sql/functions/#create_partitioned) - Create partitioned queue (requires pg_partman)
- [x] [create_unlogged](https://tembo-io.github.io/pgmq/api/sql/functions/#create_unlogged) - Create unlogged queue (faster, no crash recovery)
- [x] [drop_queue](https://tembo-io.github.io/pgmq/api/sql/functions/#drop_queue) - Delete queue and all messages
- [x] [detach_archive](https://tembo-io.github.io/pgmq/api/sql/functions/#detach_archive) - Detach archive table from queue

### Utilities
- [x] [set_vt](https://tembo-io.github.io/pgmq/api/sql/functions/#set_vt) - Update message visibility timeout
- [x] [list_queues](https://tembo-io.github.io/pgmq/api/sql/functions/#list_queues) - List all queues with metadata
- [x] [metrics](https://tembo-io.github.io/pgmq/api/sql/functions/#metrics) - Get queue metrics (length, age, total messages)
- [x] [metrics_all](https://tembo-io.github.io/pgmq/api/sql/functions/#metrics_all) - Get metrics for all queues

### Ruby-Specific Enhancements
- [x] **Transaction Support** - Atomic operations across multiple queues via `client.transaction do |txn|`
- [x] **Conditional JSONB Filtering** - Server-side message filtering using `conditional:` parameter
- [x] **Multi-Queue Operations** - Read/pop/delete/archive from multiple queues in single queries
- [x] **Enhanced Queue Validation** - 48-character limit enforcement and comprehensive name validation
- [x] **Thread-Safe Connection Pooling** - Built-in connection pool for concurrent usage
- [x] **Pluggable Serializers** - JSON (default) and MessagePack support with custom serializer API

**100% Feature Complete** - All PGMQ SQL functions are fully supported with idiomatic Ruby APIs.

## Requirements

- Ruby 3.2+
- PostgreSQL 14-18 with PGMQ extension installed

## Installation

Add to your Gemfile:

```ruby
gem 'pgmq-ruby'
```

Or install directly:

```bash
gem install pgmq-ruby
```

## Quick Start

### Basic Usage

```ruby
require 'pgmq'

# Connect to database
client = PGMQ::Client.new(
  host: 'localhost',
  port: 5432,
  dbname: 'mydb',
  user: 'postgres',
  password: 'secret'
)

# Create a queue
client.create('orders')

# Send a message
msg_id = client.send('orders', { order_id: 123, total: 99.99 })

# Read a message (30 second visibility timeout)
msg = client.read('orders', vt: 30)
puts msg.payload  # => { "order_id" => 123, "total" => 99.99 }

# Process and delete
process_order(msg.payload)
client.delete('orders', msg.msg_id)

# Or archive for long-term storage
client.archive('orders', msg.msg_id)

# Clean up
client.drop_queue('orders')
client.close
```

### Rails Integration (Reusing ActiveRecord Connection)

```ruby
# config/initializers/pgmq.rb or in your model
class OrderProcessor
  def initialize
    # Reuse Rails' connection pool - no separate connection needed!
    @client = PGMQ::Client.new(-> { ActiveRecord::Base.connection.raw_connection })
  end

  def process_orders
    loop do
      msg = @client.read('orders', vt: 30)
      break unless msg

      process_order(msg.payload)
      @client.delete('orders', msg.msg_id)
    end
  end
end
```

## Connection Options

PGMQ-Ruby supports multiple ways to connect:

### Connection Hash

```ruby
client = PGMQ::Client.new(
  host: 'localhost',
  port: 5432,
  dbname: 'mydb',
  user: 'postgres',
  password: 'secret',
  pool_size: 5,        # Default: 5
  pool_timeout: 5      # Default: 5 seconds
)
```

### Connection String

```ruby
client = PGMQ::Client.new('postgres://user:pass@localhost:5432/dbname')
```

### Environment Variables

```ruby
# Uses PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
client = PGMQ::Client.new
```

### Rails ActiveRecord (Recommended for Rails apps)

```ruby
# Reuses Rails connection pool - no additional connections needed
client = PGMQ::Client.new(-> { ActiveRecord::Base.connection.raw_connection })
```

### Custom Connection Pool

```ruby
# Bring your own connection management
connection = PGMQ::Connection.new('postgres://localhost/mydb', pool_size: 10)
client = PGMQ::Client.new(connection)
```

### Connection Pool Features

PGMQ-Ruby includes connection pooling with monitoring and resilience features:

```ruby
# Configure pool size and timeouts
client = PGMQ::Client.new(
  'postgres://localhost/mydb',
  pool_size: 10,           # Number of connections (default: 5)
  pool_timeout: 5,         # Timeout in seconds (default: 5)
  auto_reconnect: true     # Auto-reconnect on connection loss (default: true)
)

# Monitor connection pool health
stats = client.stats
puts "Pool size: #{stats[:size]}"           # => 10
puts "Available: #{stats[:available]}"      # => 8 (2 in use)

# Auto-reconnect handles connection failures gracefully
# Subscribe to reconnection events for monitoring
PGMQ::Notifications.subscribe('connection.auto_reconnect') do |event|
  logger.warn "Database reconnected: #{event.payload[:error]}"
end

# Disable auto-reconnect if you prefer explicit error handling
client = PGMQ::Client.new(auto_reconnect: false)
```

**Connection Pool Benefits:**
- **Thread-safe** - Multiple threads can safely share a single client
- **Fiber-aware** - Works with Ruby 3.0+ Fiber Scheduler for non-blocking I/O
- **Auto-reconnect** - Recovers from lost connections (configurable)
- **Health checks** - Verifies connections before use to prevent stale connection errors
- **Monitoring** - Track pool utilization with `client.stats`

### Custom Serializer

```ruby
# Use MessagePack instead of JSON
client = PGMQ::Client.new(
  'postgres://localhost/mydb',
  serializer: PGMQ::Serializers::MessagePack.new
)
```

## API Reference

### Queue Management

```ruby
# Create a queue
client.create("queue_name")

# Create partitioned queue (requires pg_partman)
client.create_partitioned("queue_name",
  partition_interval: "daily",
  retention_interval: "7 days"
)

# Create unlogged queue (faster, no crash recovery)
client.create_unlogged("queue_name")

# Drop queue
client.drop_queue("queue_name")

# List all queues
queues = client.list_queues
# => [#<PGMQ::QueueMetadata queue_name="orders" created_at=...>, ...]
```

#### Queue Naming Rules

Queue names must follow PostgreSQL identifier rules with PGMQ-specific constraints:

- **Maximum 48 characters** (PGMQ enforces this limit for table prefixes)
- Must start with a letter or underscore
- Can contain only letters, digits, and underscores
- Case-sensitive

**Valid Queue Names:**
```ruby
client.create("orders")           # ✓ Simple name
client.create("high_priority")    # ✓ With underscore
client.create("Queue123")         # ✓ With numbers
client.create("_internal")        # ✓ Starts with underscore
client.create("a" * 47)          # ✓ Maximum length (47 chars)
```

**Invalid Queue Names:**
```ruby
client.create("123orders")        # ✗ Starts with number
client.create("my-queue")         # ✗ Contains hyphen
client.create("my.queue")         # ✗ Contains period
client.create("a" * 48)          # ✗ Too long (48+ chars)
# Raises PGMQ::InvalidQueueNameError
```

### Sending Messages

```ruby
# Send single message
msg_id = client.send("queue_name", { data: "value" })

# Send with delay (seconds)
msg_id = client.send("queue_name", { data: "value" }, delay: 60)

# Send batch
msg_ids = client.send_batch("queue_name", [
  { order: 1 },
  { order: 2 },
  { order: 3 }
])
# => [101, 102, 103]
```

### Reading Messages

```ruby
# Read single message
msg = client.read("queue_name", vt: 30)
# => #<PGMQ::Message msg_id=1 payload={...}>

# Read batch
messages = client.read_batch("queue_name", vt: 30, qty: 10)

# Read with long-polling
msg = client.read_with_poll("queue_name",
  vt: 30,
  qty: 1,
  max_poll_seconds: 5,
  poll_interval_ms: 100
)

# Pop (atomic read + delete)
msg = client.pop("queue_name")
```

#### Conditional Message Filtering (v0.3.0+)

Filter messages by JSON payload content using server-side JSONB queries:

```ruby
# Filter by single condition
msg = client.read("orders", vt: 30, conditional: { status: "pending" })

# Filter by multiple conditions (AND logic)
msg = client.read("orders", vt: 30, conditional: {
  status: "pending",
  priority: "high"
})

# Filter by nested properties
msg = client.read("orders", vt: 30, conditional: {
  user: { role: "admin" }
})

# Works with read_batch
messages = client.read_batch("orders",
  vt: 30,
  qty: 10,
  conditional: { type: "priority" }
)

# Works with long-polling
messages = client.read_with_poll("orders",
  vt: 30,
  max_poll_seconds: 5,
  conditional: { status: "ready" }
)
```

**How Filtering Works:**
- Filtering happens in PostgreSQL using JSONB containment operator (`@>`)
- Only messages matching **ALL** conditions are returned (AND logic)
- The `qty` parameter applies **after** filtering
- Empty conditions `{}` means no filtering (same as omitting parameter)

**Performance Tip:** For frequently filtered fields, add JSONB indexes:
```sql
CREATE INDEX idx_orders_status
  ON pgmq.q_orders USING gin ((message->'status'));
```

### Message Lifecycle

```ruby
# Delete message
client.delete("queue_name", msg_id)

# Delete batch
deleted_ids = client.delete_batch("queue_name", [101, 102, 103])

# Archive message
client.archive("queue_name", msg_id)

# Archive batch
archived_ids = client.archive_batch("queue_name", [101, 102, 103])

# Update visibility timeout
msg = client.set_vt("queue_name", msg_id, vt_offset: 60)

# Purge all messages
count = client.purge_queue("queue_name")
```

### Monitoring

```ruby
# Get queue metrics
metrics = client.metrics("queue_name")
puts metrics.queue_length        # => 42
puts metrics.oldest_msg_age_sec  # => 120
puts metrics.newest_msg_age_sec  # => 5
puts metrics.total_messages      # => 1000

# Get all queue metrics
all_metrics = client.metrics_all
all_metrics.each do |m|
  puts "#{m.queue_name}: #{m.queue_length} messages"
end
```

### Transaction Support (v0.3.0+)

Low-level PostgreSQL transaction support for atomic operations. Transactions are a database primitive provided by PostgreSQL - this is a thin wrapper for convenience.

Execute atomic operations across multiple queues or combine queue operations with application data updates:

```ruby
# Atomic operations across multiple queues
client.transaction do |txn|
  # Send to multiple queues atomically
  txn.send("orders", { order_id: 123 })
  txn.send("notifications", { user_id: 456, type: "order_created" })
  txn.send("analytics", { event: "order_placed" })
end

# Process message and update application state atomically
client.transaction do |txn|
  # Read and process message
  msg = txn.read("orders", vt: 30)

  if msg
    # Update your database
    Order.create!(external_id: msg.payload["order_id"])

    # Delete message only if database update succeeds
    txn.delete("orders", msg.msg_id)
  end
end

# Automatic rollback on errors
client.transaction do |txn|
  txn.send("queue1", { data: "message1" })
  txn.send("queue2", { data: "message2" })

  raise "Something went wrong!"
  # Both messages are rolled back - neither queue receives anything
end

# Move messages between queues atomically
client.transaction do |txn|
  msg = txn.read("pending_orders", vt: 30)

  if msg && msg.payload["priority"] == "high"
    # Move to high-priority queue
    txn.send("priority_orders", msg.payload)
    txn.delete("pending_orders", msg.msg_id)
  end
end
```

**How Transactions Work:**
- Wraps PostgreSQL's native transaction support (similar to rdkafka-ruby providing Kafka transactions)
- All operations within the block execute in a single PostgreSQL transaction
- If any operation fails, the entire transaction is rolled back automatically
- The transactional client delegates all `PGMQ::Client` methods for convenience

**Use Cases:**
- **Multi-queue coordination**: Send related messages to multiple queues atomically
- **Exactly-once processing**: Combine message deletion with application state updates
- **Message routing**: Move messages between queues without losing data
- **Batch operations**: Ensure all-or-nothing semantics for bulk operations

**Important Notes:**
- Transactions hold database locks - keep them short to avoid blocking
- Long transactions can impact queue throughput
- Read operations with long visibility timeouts may cause lock contention
- Consider using `pop()` for atomic read+delete in simple cases

## Message Object

```ruby
msg = client.read("queue", vt: 30)

msg.msg_id          # => 123 (bigint)
msg.read_ct         # => 1 (read count)
msg.enqueued_at     # => 2025-01-15 10:30:00 UTC
msg.vt              # => 2025-01-15 10:30:30 UTC (visibility timeout)
msg.message         # => { "data" => "value" }
msg.payload         # => alias for message

# Hash-like access
msg[:data]          # => "value"
msg["data"]         # => "value"
```

## Serializers

PGMQ-Ruby supports pluggable serializers for message payloads.

### JSON (Default)

```ruby
client = PGMQ::Client.new('postgres://localhost/mydb')
# Uses JSON serializer by default
```

### MessagePack

```ruby
# Gemfile
gem 'msgpack'

# Use MessagePack for better performance
client = PGMQ::Client.new(
  'postgres://localhost/mydb',
  serializer: PGMQ::Serializers::MessagePack.new
)
```

### Custom Serializer

```ruby
class MySerializer < PGMQ::Serializers::Base
  def serialize(obj)
    # Convert Ruby object to string for storage
    obj.to_json
  end

  def deserialize(str)
    # Convert stored string back to Ruby object
    JSON.parse(str)
  end
end

client = PGMQ::Client.new(
  'postgres://localhost/mydb',
  serializer: MySerializer.new
)
```

### Setup (v1.0+)

```bash
# Generate initializer (coming in v1.0)
rails generate pgmq:install

# Creates: config/initializers/pgmq.rb
```

### ActiveJob Adapter (v1.0+)

```ruby
# config/application.rb
config.active_job.queue_adapter = :pgmq

# Job automatically uses PGMQ
class ProcessOrderJob < ApplicationJob
  queue_as :orders

  def perform(order_id)
    order = Order.find(order_id)
    # Process order
  end
end
```

### Rake Tasks (v1.0+)

```bash
# Create queue
rake pgmq:create_queue[orders]

# Drop queue
rake pgmq:drop_queue[orders]

# Show metrics
rake pgmq:metrics[orders]

# Show all queue metrics
rake pgmq:metrics_all
```

**Current Workaround**: Use the PGMQ::Client directly in your Rails app until v1.0 is released. See the standalone examples above.

## Testing

### With Docker

```bash
# Start PostgreSQL with PGMQ extension
docker compose up -d

# Run tests
bundle exec rspec
```

### docker-compose.yml

```yaml
services:
  postgres:
    image: quay.io/tembo/pg17-pgmq:latest
    environment:
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
```

### Test Helpers

```ruby
RSpec.configure do |config|
  config.before(:suite) do
    PGMQ.configure do |c|
      c.connection_string = "postgres://postgres:postgres@localhost:5432/pgmq_test"
    end

    @client = PGMQ::Client.new
  end

  config.before(:each) do
    # Clean up queues between tests
  end
end
```

## Performance

PGMQ-Ruby is designed for high throughput while maintaining simplicity.

> **Note**: Formal benchmarks are planned for v1.0. The following are preliminary estimates based on similar PGMQ clients and PostgreSQL capabilities.

**Estimated Performance** (Ruby 3.3, PostgreSQL 16, localhost):

- **Send**: ~5,000-10,000 messages/sec (single)
- **Send batch**: ~20,000-40,000 messages/sec (batch of 100)
- **Read**: ~8,000-15,000 messages/sec
- **Delete**: ~10,000-20,000 operations/sec

Real-world performance depends on:
- PostgreSQL configuration (shared_buffers, work_mem, etc.)
- Network latency
- Message payload size
- Connection pool size
- Whether queues are partitioned or unlogged

Run your own benchmarks for accurate numbers in your environment.

## Comparison with Other Solutions

| Feature | PGMQ-Ruby | Sidekiq | DelayedJob | AWS SQS |
|---------|-----------|---------|------------|---------|
| Infrastructure | PostgreSQL | Redis | PostgreSQL | AWS Cloud |
| Exactly-once delivery | ✓ | ✗ | ✓ | ✓ |
| Visibility timeout | ✓ | ✗ | ✗ | ✓ |
| Message archiving | ✓ | ✗ | ✗ | ✗ |
| Operational cost | Low | Low | Low | Pay-per-use |
| Learning curve | Low | Medium | Low | Medium |

**Use PGMQ when:**
- You already use PostgreSQL
- You want SQS-like semantics without AWS
- You need exactly-once delivery guarantees
- You want to avoid Redis dependency

**Use Sidekiq when:**
- You need advanced job scheduling
- You already use Redis
- You need web UI for monitoring

## Development

```bash
# Clone repository
git clone https://github.com/mensfeld/pgmq-ruby.git
cd pgmq-ruby

# Install dependencies
bundle install

# Start PostgreSQL with PGMQ
docker compose up -d

# Run tests
bundle exec rspec

# Run console
bundle exec bin/console
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mensfeld/pgmq-ruby.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/my-new-feature`)
5. Create a new Pull Request

## Resources

- [PGMQ Documentation](https://github.com/pgmq/pgmq)
- [PGMQ Extension](https://github.com/pgmq/pgmq)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## License

The gem is available as open source under the terms of the [LGPL-3.0 License](https://opensource.org/licenses/LGPL-3.0).

## Author

Maintained by [Maciej Mensfeld](https://github.com/mensfeld)

Also check out [Karafka](https://karafka.io) - High-performance Apache Kafka framework for Ruby.
