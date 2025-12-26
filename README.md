# PGMQ-Ruby

[![Gem Version](https://badge.fury.io/rb/pgmq-ruby.svg)](https://badge.fury.io/rb/pgmq-ruby)
[![Build Status](https://github.com/mensfeld/pgmq-ruby/workflows/CI/badge.svg)](https://github.com/mensfeld/pgmq-ruby/actions)

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
- [Development](#development)
- [License](#license)
- [Author](#author)

## PGMQ Feature Support

This gem provides complete support for all core PGMQ SQL functions. Based on the [official PGMQ API](https://pgmq.github.io/pgmq/):

| Category | Method | Description | Status |
|----------|--------|-------------|--------|
| **Producing** | `produce` | Send single message with optional delay and headers | ✅ |
| | `produce_batch` | Send multiple messages atomically with headers | ✅ |
| **Reading** | `read` | Read single message with visibility timeout | ✅ |
| | `read_batch` | Read multiple messages with visibility timeout | ✅ |
| | `read_with_poll` | Long-polling for efficient message consumption | ✅ |
| | `pop` | Atomic read + delete operation | ✅ |
| | `pop_batch` | Atomic batch read + delete operation | ✅ |
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
| | `set_vt_batch` | Batch update visibility timeouts | ✅ |
| | `set_vt_multi` | Update visibility timeouts across multiple queues | ✅ |
| | `list_queues` | List all queues with metadata | ✅ |
| | `metrics` | Get queue metrics (length, age, total messages) | ✅ |
| | `metrics_all` | Get metrics for all queues | ✅ |
| | `enable_notify_insert` | Enable PostgreSQL NOTIFY on insert | ✅ |
| | `disable_notify_insert` | Disable notifications | ✅ |
| **Ruby Enhancements** | Transaction Support | Atomic operations via `client.transaction do \|txn\|` | ✅ |
| | Conditional Filtering | Server-side JSONB filtering with `conditional:` | ✅ |
| | Multi-Queue Ops | Read/pop/delete/archive from multiple queues | ✅ |
| | Queue Validation | 48-character limit and name validation | ✅ |
| | Connection Pooling | Thread-safe connection pool for concurrency | ✅ |
| | Pluggable Serializers | JSON (default) with custom serializer support | ✅ |

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

# Send a message (must be JSON string)
msg_id = client.produce('orders', '{"order_id":123,"total":99.99}')

# Read a message (30 second visibility timeout)
msg = client.read('orders', vt: 30)
puts msg.message  # => "{\"order_id\":123,\"total\":99.99}" (raw JSON string)

# Parse and process (you handle deserialization)
data = JSON.parse(msg.message)
process_order(data)
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

      # Parse JSON yourself
      data = JSON.parse(msg.message)
      process_order(data)
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


## API Reference

### Queue Management

```ruby
# Create a queue (returns true if created, false if already exists)
client.create("queue_name")      # => true
client.create("queue_name")      # => false (idempotent)

# Create partitioned queue (requires pg_partman)
client.create_partitioned("queue_name",
  partition_interval: "daily",
  retention_interval: "7 days"
)  # => true/false

# Create unlogged queue (faster, no crash recovery)
client.create_unlogged("queue_name")  # => true/false

# Drop queue (returns true if dropped, false if didn't exist)
client.drop_queue("queue_name")  # => true/false

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
# Raises PGMQ::Errors::InvalidQueueNameError
```

### Sending Messages

```ruby
# Send single message (must be JSON string)
msg_id = client.produce("queue_name", '{"data":"value"}')

# Send with delay (seconds)
msg_id = client.produce("queue_name", '{"data":"value"}', delay: 60)

# Send with headers (for routing, tracing, correlation)
msg_id = client.produce("queue_name", '{"data":"value"}',
  headers: '{"trace_id":"abc123","priority":"high"}')

# Send with headers and delay
msg_id = client.produce("queue_name", '{"data":"value"}',
  headers: '{"correlation_id":"req-456"}',
  delay: 60)

# Send batch (array of JSON strings)
msg_ids = client.produce_batch("queue_name", [
  '{"order":1}',
  '{"order":2}',
  '{"order":3}'
])
# => ["101", "102", "103"]

# Send batch with headers (one per message)
msg_ids = client.produce_batch("queue_name",
  ['{"order":1}', '{"order":2}'],
  headers: ['{"priority":"high"}', '{"priority":"low"}'])
```

### Reading Messages

```ruby
# Read single message
msg = client.read("queue_name", vt: 30)
# => #<PGMQ::Message msg_id="1" message="{...}">

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

# Pop batch (atomic read + delete for multiple messages)
messages = client.pop_batch("queue_name", 10)
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

# Batch update visibility timeout
updated_msgs = client.set_vt_batch("queue_name", [101, 102, 103], vt_offset: 60)

# Update visibility timeout across multiple queues
client.set_vt_multi({
  "orders" => [1, 2, 3],
  "notifications" => [5, 6]
}, vt_offset: 120)

# Purge all messages
count = client.purge_queue("queue_name")

# Enable PostgreSQL NOTIFY for a queue (for LISTEN-based consumers)
client.enable_notify_insert("queue_name", throttle_interval_ms: 250)

# Disable notifications
client.disable_notify_insert("queue_name")
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
  txn.produce("orders", '{"order_id":123}')
  txn.produce("notifications", '{"user_id":456,"type":"order_created"}')
  txn.produce("analytics", '{"event":"order_placed"}')
end

# Process message and update application state atomically
client.transaction do |txn|
  # Read and process message
  msg = txn.read("orders", vt: 30)

  if msg
    # Parse and update your database
    data = JSON.parse(msg.message)
    Order.create!(external_id: data["order_id"])

    # Delete message only if database update succeeds
    txn.delete("orders", msg.msg_id)
  end
end

# Automatic rollback on errors
client.transaction do |txn|
  txn.produce("queue1", '{"data":"message1"}')
  txn.produce("queue2", '{"data":"message2"}')

  raise "Something went wrong!"
  # Both messages are rolled back - neither queue receives anything
end

# Move messages between queues atomically
client.transaction do |txn|
  msg = txn.read("pending_orders", vt: 30)

  if msg
    data = JSON.parse(msg.message)
    if data["priority"] == "high"
      # Move to high-priority queue
      txn.produce("priority_orders", msg.message)
      txn.delete("pending_orders", msg.msg_id)
    end
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

PGMQ-Ruby is a **low-level transport library** - it returns raw values from PostgreSQL without any transformation. You are responsible for parsing JSON and type conversion.

```ruby
msg = client.read("queue", vt: 30)

# All values are strings as returned by PostgreSQL
msg.msg_id          # => "123" (String, not Integer)
msg.id              # => "123" (alias for msg_id)
msg.read_ct         # => "1" (String, not Integer)
msg.enqueued_at     # => "2025-01-15 10:30:00+00" (String, not Time)
msg.vt              # => "2025-01-15 10:30:30+00" (String, not Time)
msg.message         # => "{\"data\":\"value\"}" (Raw JSONB as JSON string)
msg.headers         # => "{\"trace_id\":\"abc123\"}" (Raw JSONB as JSON string, optional)
msg.queue_name      # => "my_queue" (only present for multi-queue operations, otherwise nil)

# You handle JSON parsing
data = JSON.parse(msg.message)  # => { "data" => "value" }
metadata = JSON.parse(msg.headers) if msg.headers  # => { "trace_id" => "abc123" }

# You handle type conversion if needed
id = msg.msg_id.to_i           # => 123
read_count = msg.read_ct.to_i  # => 1
enqueued = Time.parse(msg.enqueued_at)  # => 2025-01-15 10:30:00 UTC
```

### Message Headers

PGMQ supports optional message headers via the `headers` JSONB column. Headers are useful for metadata like routing information, correlation IDs, and distributed tracing:

```ruby
# Sending a message with headers
message = '{"order_id":123}'
headers = '{"trace_id":"abc123","priority":"high","correlation_id":"req-456"}'

msg_id = client.produce("orders", message, headers: headers)

# Sending with headers and delay
msg_id = client.produce("orders", message, headers: headers, delay: 60)

# Batch produce with headers (one header object per message)
messages = ['{"id":1}', '{"id":2}', '{"id":3}']
headers = [
  '{"priority":"high"}',
  '{"priority":"medium"}',
  '{"priority":"low"}'
]
msg_ids = client.produce_batch("orders", messages, headers: headers)

# Reading messages with headers
msg = client.read("orders", vt: 30)
if msg.headers
  metadata = JSON.parse(msg.headers)
  trace_id = metadata["trace_id"]
  priority = metadata["priority"]
  correlation_id = metadata["correlation_id"]
end
```

Common header use cases:
- **Distributed tracing**: `trace_id`, `span_id`, `parent_span_id`
- **Request correlation**: `correlation_id`, `causation_id`
- **Routing**: `priority`, `region`, `tenant_id`
- **Content metadata**: `content_type`, `encoding`, `version`

### Why Raw Values?

This library follows the **rdkafka-ruby philosophy** - provide a thin, performant wrapper around the underlying system:

1. **No assumptions** - Your application decides how to parse timestamps, convert types, etc.
2. **Framework-agnostic** - Works equally well with Rails, Sinatra, or plain Ruby
3. **Zero overhead** - No hidden type conversion or object allocation
4. **Explicit control** - You see exactly what PostgreSQL returns

Higher-level features (automatic deserialization, type conversion, instrumentation) belong in framework layers built on top of this library.

## Working with JSON

PGMQ stores messages as JSONB in PostgreSQL. You must handle JSON serialization yourself:

### Sending Messages

```ruby
# Simple hash
msg = { order_id: 123, status: "pending" }
client.produce("orders", msg.to_json)

# Using JSON.generate for explicit control
client.produce("orders", JSON.generate(order_id: 123, status: "pending"))

# Pre-serialized JSON string
json_str = '{"order_id":123,"status":"pending"}'
client.produce("orders", json_str)
```

### Reading Messages

```ruby
msg = client.read("orders", vt: 30)

# Parse JSON yourself
data = JSON.parse(msg.message)
puts data["order_id"]  # => 123
puts data["status"]    # => "pending"

# Handle parsing errors
begin
  data = JSON.parse(msg.message)
rescue JSON::ParserError => e
  logger.error "Invalid JSON in message #{msg.msg_id}: #{e.message}"
  client.delete("orders", msg.msg_id)  # Remove invalid message
end
```

### Helper Pattern (Optional)

For convenience, you can wrap the client in your own helper:

```ruby
class QueueHelper
  def initialize(client)
    @client = client
  end

  def produce(queue, data)
    @client.produce(queue, data.to_json)
  end

  def read(queue, vt:)
    msg = @client.read(queue, vt: vt)
    return nil unless msg

    OpenStruct.new(
      id: msg.msg_id.to_i,
      data: JSON.parse(msg.message),
      read_count: msg.read_ct.to_i,
      raw: msg
    )
  end
end

helper = QueueHelper.new(client)
helper.produce("orders", { order_id: 123 })
msg = helper.read("orders", vt: 30)
puts msg.data["order_id"]  # => 123
```

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
