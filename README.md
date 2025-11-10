# PGMQ-Ruby

[![Gem Version](https://badge.fury.io/rb/pgmq-ruby.svg)](https://badge.fury.io/rb/pgmq-ruby)
[![Build Status](https://github.com/mensfeld/pgmq-ruby/workflows/ci/badge.svg)](https://github.com/mensfeld/pgmq-ruby/actions)

**Ruby client for [PGMQ](https://github.com/pgmq/pgmq) - PostgreSQL Message Queue**

## What is PGMQ-Ruby?

PGMQ-Ruby is a Ruby client for PGMQ (PostgreSQL Message Queue). It provides direct access to all PGMQ operations with a clean, minimal API - similar to how [rdkafka-ruby](https://github.com/karafka/rdkafka-ruby) relates to Kafka.

**Think of it as:**

- **Like AWS SQS** - but running entirely in PostgreSQL with no external dependencies
- **Like Sidekiq/Resque** - but without Redis, using PostgreSQL for both data and queues
- **Like rdkafka-ruby** - a thin, efficient wrapper around the underlying system (PGMQ SQL functions)

> **Architecture Note**: This library follows the rdkafka-ruby/Karafka pattern - `pgmq-ruby` is the low-level foundation, while higher-level features (job processing, Rails integration, retry strategies) will live in `pgmq-framework` (similar to how Karafka builds on rdkafka-ruby).

## Table of Contents

- [PGMQ Feature Support](#pgmq-feature-support)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Message Object](#message-object)
- [Serializers](#serializers)
- [Rails Integration](#rails-integration)
- [Performance](#performance)
- [Future Improvements](#future-improvements)
- [Development](#development)
- [License](#license)
- [Author](#author)

## PGMQ Feature Support

This gem provides complete support for all core PGMQ SQL functions. Based on the [official PGMQ API](https://pgmq.github.io/pgmq/):

| Category | Method | Description | Status |
|----------|--------|-------------|--------|
| **Sending** | `send` | Send single message with optional delay | ✅ |
| | `send_batch` | Send multiple messages atomically | ✅ |
| **Reading** | `read` | Read single message with visibility timeout | ✅ |
| | `read_batch` | Read multiple messages with visibility timeout | ✅ |
| | `read_with_poll` | Long-polling for efficient message consumption | ✅ |
| | `pop` | Atomic read + delete operation | ✅ |
| **Deleting/Archiving** | `delete` | Delete single message | ✅ |
| | `delete_batch` | Delete multiple messages | ✅ |
| | `archive` | Archive single message for long-term storage | ✅ |
| | `archive_batch` | Archive multiple messages | ✅ |
| | `purge_queue` | Remove all messages from queue | ✅ |
| **Queue Management** | `create` | Create standard queue | ✅ |
| | `create_partitioned` | Create partitioned queue (requires pg_partman) | ✅ |
| | `create_unlogged` | Create unlogged queue (faster, no crash recovery) | ✅ |
| | `drop_queue` | Delete queue and all messages | ✅ |
| | `detach_archive` | Detach archive table from queue | ✅ |
| **Utilities** | `set_vt` | Update message visibility timeout | ✅ |
| | `list_queues` | List all queues with metadata | ✅ |
| | `metrics` | Get queue metrics (length, age, total messages) | ✅ |
| | `metrics_all` | Get metrics for all queues | ✅ |
| **Ruby Enhancements** | Transaction Support | Atomic operations via `client.transaction do \|txn\|` | ✅ |
| | Conditional Filtering | Server-side JSONB filtering with `conditional:` | ✅ |
| | Multi-Queue Ops | Read/pop/delete/archive from multiple queues | ✅ |
| | Queue Validation | 48-character limit and name validation | ✅ |
| | Connection Pooling | Thread-safe connection pool for concurrency | ✅ |
| | Pluggable Serializers | JSON (default) and MessagePack support | ✅ |

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

PGMQ-Ruby includes connection pooling with resilience:

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

# Disable auto-reconnect if you prefer explicit error handling
client = PGMQ::Client.new(
  'postgres://localhost/mydb',
  auto_reconnect: false
)
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

#### Conditional Message Filtering

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

### Transaction Support

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

## Future Improvements

PGMQ-Ruby provides 100% coverage of core PGMQ SQL functions. The following advanced features are available in PGMQ but not yet implemented in this low-level client:

### Archive Partitioning Conversion

**What it is:** `convert_archive_partitioned()` - Convert existing non-partitioned archive tables to partitioned ones.

**Use case:** Optimize performance for queues with large archive tables accumulated over time.

**Current workaround:** Use `create_partitioned()` when creating queues that will accumulate large archives.

**Status:** Low priority. Users can create partitioned queues from the start.

---

**Note:** PGMQ-Ruby already provides complete coverage of all essential queue operations and is production-ready. Advanced features like PostgreSQL LISTEN/NOTIFY support, worker process management, and automatic retries will be available in `pgmq-framework` (coming soon).

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

## Author

Maintained by [Maciej Mensfeld](https://github.com/mensfeld)

Also check out [Karafka](https://karafka.io) - High-performance Apache Kafka framework for Ruby.
