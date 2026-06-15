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
  - [Queue Management](#queue-management)
  - [Sending Messages](#sending-messages)
  - [Reading Messages](#reading-messages)
  - [Grouped Reading](#grouped-reading)
  - [Message Lifecycle](#message-lifecycle)
  - [Monitoring](#monitoring)
  - [Transaction Support](#transaction-support)
  - [Topic Routing](#topic-routing-amqp-like-patterns)
- [Message Object](#message-object)
- [Working with JSON](#working-with-json)
- [Development](#development)
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
| | `read_grouped` | SQS-style throughput-first grouped reading | ✅ |
| | `read_grouped_with_poll` | Throughput-first grouped reading with long-polling | ✅ |
| | `read_grouped_rr` | Round-robin reading across message groups | ✅ |
| | `read_grouped_rr_with_poll` | Round-robin with long-polling | ✅ |
| | `read_grouped_head` | One message per FIFO group from the head of each group | ✅ |
| | `pop` | Atomic read + delete operation | ✅ |
| | `pop_batch` | Atomic batch read + delete operation | ✅ |
| **Deleting/Archiving** | `delete` | Delete single message | ✅ |
| | `delete_batch` | Delete multiple messages | ✅ |
| | `archive` | Archive single message for long-term storage | ✅ |
| | `archive_batch` | Archive multiple messages | ✅ |
| | `purge_queue` | Remove all messages from queue | ✅ |
| | `convert_archive_partitioned` | Convert archive table to pg_partman-managed partitions | ✅ |
| **Queue Management** | `create` | Create standard queue | ✅ |
| | `create_partitioned` | Create partitioned queue (requires pg_partman) | ✅ |
| | `create_unlogged` | Create unlogged queue (faster, no crash recovery) | ✅ |
| | `create_fifo_index` | Create FIFO index required for grouped reads | ✅ |
| | `create_fifo_indexes_all` | Create FIFO indexes on all existing queues | ✅ |
| | `drop_queue` | Delete queue and all messages | ✅ |
| **Topic Routing** | `bind_topic` | Bind topic pattern to queue (AMQP-like) | ✅ |
| | `unbind_topic` | Remove topic binding | ✅ |
| | `produce_topic` | Send message via routing key | ✅ |
| | `produce_batch_topic` | Batch send via routing key | ✅ |
| | `list_topic_bindings` | List all topic bindings | ✅ |
| | `test_routing` | Test which queues match a routing key | ✅ |
| **Utilities** | `set_vt` | Update visibility timeout (integer or Time) | ✅ |
| | `set_vt_batch` | Batch update visibility timeouts | ✅ |
| | `set_vt_multi` | Update visibility timeouts across multiple queues | ✅ |
| | `list_queues` | List all queues with metadata | ✅ |
| | `metrics` | Get queue metrics (length, age, total messages) | ✅ |
| | `metrics_all` | Get metrics for all queues | ✅ |
| | `enable_notify_insert` | Enable PostgreSQL NOTIFY on insert | ✅ |
| | `update_notify_insert` | Update throttle interval on an existing NOTIFY trigger | ✅ |
| | `disable_notify_insert` | Disable notifications | ✅ |
| | `list_notify_insert_throttles` | List all queues with NOTIFY enabled and their throttle config | ✅ |
| | `wait_for_notify` | Block until a NOTIFY arrives on the queue's channel | ✅ |
| **Ruby Enhancements** | Transaction Support | Atomic operations via `client.transaction do \|txn\|` | ✅ |
| | Conditional Filtering | Server-side JSONB filtering with `conditional:` | ✅ |
| | Multi-Queue Ops | Read/pop/delete/archive from multiple queues | ✅ |
| | Queue Validation | 48-character limit and name validation | ✅ |
| | Connection Pooling | Thread-safe connection pool for concurrency | ✅ |
| | Pluggable Serializers | JSON (default) with custom serializer support | ✅ |

## Requirements

- Ruby 3.2+
- PostgreSQL 14-18 with PGMQ extension installed

### Installing PGMQ Extension

PGMQ can be installed on your PostgreSQL instance in several ways:

#### Standard Installation (Self-hosted PostgreSQL)

For self-hosted PostgreSQL instances with filesystem access, install via [PGXN](https://pgxn.org/dist/pgmq/):

```bash
pgxn install pgmq
```

Or build from source:

```bash
git clone https://github.com/pgmq/pgmq.git
cd pgmq/pgmq-extension
make && make install
```

Then enable the extension:

```sql
CREATE EXTENSION pgmq;
```

#### Managed PostgreSQL Services (AWS RDS, Aurora, etc.)

For managed PostgreSQL services that don't allow native extension installation, PGMQ provides a **SQL-only installation** that works without filesystem access:

```bash
git clone https://github.com/pgmq/pgmq.git
cd pgmq
psql -f pgmq-extension/sql/pgmq.sql postgres://user:pass@your-rds-host:5432/database
```

This creates a `pgmq` schema with all required functions. See [PGMQ Installation Guide](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md) for details.

**Comparison:**

| Feature | Extension | SQL-only |
|---------|-----------|----------|
| Version tracking | Yes | No |
| Upgrade path | Yes | Manual |
| Filesystem access | Required | Not needed |
| Managed cloud services | Limited | Full support |

#### Using pg_tle (Trusted Language Extensions)

If your managed PostgreSQL service supports [pg_tle](https://github.com/aws/pg_tle) (available on AWS RDS PostgreSQL 14.5+ and Aurora), you can potentially install PGMQ as a Trusted Language Extension since PGMQ is written in PL/pgSQL and SQL (both supported by pg_tle).

To use pg_tle:

1. Enable pg_tle on your instance (add to `shared_preload_libraries`)
2. Create the pg_tle extension: `CREATE EXTENSION pg_tle;`
3. Use `pgtle.install_extension()` to install PGMQ's SQL functions

See [AWS pg_tle documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL_trusted_language_extension.html) for setup instructions.

> **Note:** The SQL-only installation is simpler and recommended for most managed service use cases. pg_tle provides additional version management and extension lifecycle features if needed.

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

#### Running custom SQL on the PGMQ pool

Every PGMQ operation checks out a pooled connection through `client.with_connection`.
The method is public, so you can run PostgreSQL statements PGMQ does not wrap
without standing up a second connection pool. The connection is health-checked
(when `auto_reconnect` is enabled) and returned to the pool when the block exits.

```ruby
# Fire a custom NOTIFY alongside your queues
client.with_connection do |conn|
  conn.exec_params("SELECT pg_notify($1, $2)", ["my_channel", payload])
end

# Run a monitoring query PGMQ does not wrap
depth = client.with_connection do |conn|
  conn.exec("SELECT count(*) FROM pgmq.q_orders")[0]["count"].to_i
end
```

You receive the raw `PG::Connection`: results come back as strings (no type
mapping) and the statement is **not** wrapped in a transaction. Use
[`client.transaction`](#transaction-support) when you need atomicity.

> **Pool-safety caveats.** You're handed a *pooled* connection, so two rules apply:
>
> 1. **Don't keep the connection past the block.** Once the block returns, the
>    connection goes back to the pool and another thread may check it out.
>    `PG::Connection` is not thread-safe — using it afterwards can corrupt libpq
>    state (nil results, wrong data, segfaults).
> 2. **Clean up session state before the block exits.** The pool does *not* reset
>    connections on check-in. A `LISTEN`, `SET`, session-level advisory lock
>    (`pg_advisory_lock`), prepared statement, or temp table you create survives
>    and leaks to the next pool user. Undo it (`UNLISTEN`, `RESET`,
>    `pg_advisory_unlock`, …) before returning. For LISTEN/NOTIFY consumption,
>    prefer `client.wait_for_notify`, which manages `LISTEN`/`UNLISTEN` for you.

#### Extending the lost-connection error matchers

PGMQ-Ruby ships with a curated list of `PG::Error` messages and classes
(`PG::ConnectionBad`, `PG::UnableToSend`) that trigger the auto-reconnect
retry. Different `pg` gem versions, PostgreSQL versions, and connection
poolers (PgBouncer, Supabase, RDS Proxy, etc.) occasionally surface new
disconnect signatures. Rather than wait for an upstream patch, you can
extend the matchers at boot time via class-level configuration:

```ruby
# In an initializer (e.g. config/initializers/pgmq.rb for Rails apps)
# Strings are matched as case-insensitive substrings against the error
# message; Regexps are matched against the original message.
PGMQ::Connection.reconnectable_error_patterns = [
  "connection reset by peer",
  /\Abroken pipe\b/i
]

# Any Exception subclass is accepted. Subclasses also match.
PGMQ::Connection.reconnectable_error_classes = [PG::ConnectionRefused]
```

The built-in defaults are always kept - your patterns and classes are
appended to them. Configuration errors (e.g. passing an Integer as a
pattern) raise `PGMQ::Errors::ConfigurationError` immediately so
misconfiguration can't silently disable retries.

> **Reserve these options for connection-level failures.** A "reconnectable"
> error means the socket is dead and a retry on a fresh connection is safe.
> Do **not** add patterns or classes for query-level errors (deadlocks,
> constraint violations, statement timeouts) - those will replay your
> operation against a healthy connection and may cause duplicate work or
> mask bugs.

**Connection Pool Benefits:**
- **Thread-safe** - Multiple threads can safely share a single client
- **Fiber-aware** - Works with Ruby 3.0+ Fiber Scheduler for non-blocking I/O (tested with the `async` gem)
- **Auto-reconnect** - Recovers from lost connections (configurable, extendable)
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

# Create a queue with autovacuum tuned for PGMQ's read+delete churn (see "Autovacuum Tuning")
client.create("queue_name", tune_autovacuum: true)

# Drop queue (returns true if dropped, false if didn't exist)
client.drop_queue("queue_name")  # => true/false

# Create the FIFO index required for grouped reads (read_grouped, read_grouped_rr, read_grouped_head)
# Idempotent - safe to call on a queue that already has the index
client.create_fifo_index("queue_name")

# Create FIFO indexes for all existing queues at once (useful for migrating an existing deployment)
client.create_fifo_indexes_all

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

**Deriving valid names with `PGMQ::QueueName`**

`PGMQ::Client` always validates the name you pass (via `PGMQ::QueueName.validate!`).
When the name comes from a friendlier source — a Turbo Stream channel, a slug, or
untrusted user input — `PGMQ::QueueName` gives you a few tiers so you can decide
how strict to be:

```ruby
# Tier 1 — assert a name you control is valid (raises otherwise)
PGMQ::QueueName.valid?("orders")          # => true
PGMQ::QueueName.validate!("my-queue")     # => raises PGMQ::Errors::InvalidQueueNameError

# Tier 2 — normalize a name meant to be valid but using friendly separators.
# Hyphens/dots/colons become underscores; any OTHER invalid char is stripped;
# raises if the result still can't be valid (empty, or starts with a digit).
PGMQ::QueueName.normalize("chat:room-7")  # => "chat_room_7"
PGMQ::QueueName.normalize("order.events") # => "order_events"
PGMQ::QueueName.normalize("a@b")          # => "ab"   (the "@" is dropped, not turned into "_")
PGMQ::QueueName.normalize("123-go")       # => raises (starts with a digit)

# Tier 3 — sanitize! untrusted input: strip every invalid char, then validate.
# Raises rather than substituting, so it never silently points at a different
# queue. Use this as a SQL-identifier guard for untrusted input.
PGMQ::QueueName.sanitize!("orders!!")     # => "orders"
PGMQ::QueueName.sanitize!("!!!")          # => raises PGMQ::Errors::InvalidQueueNameError

# Tier 3 (lenient) — sanitize never raises; always returns a valid name (prefixes
# leading digits, truncates to length, falls back to "queue"). Convenient, but
# distinct inputs can map to the SAME name, so prefer sanitize! for untrusted input.
PGMQ::QueueName.sanitize("99 Problems!")  # => "q_99_problems"
PGMQ::QueueName.sanitize("!!!")           # => "queue"

client.create(PGMQ::QueueName.sanitize!(params[:topic]))  # raises on junk rather than guessing
```

#### Autovacuum Tuning

PGMQ tables churn in a way PostgreSQL's defaults are not tuned for: a hot queue
inserts, updates (visibility timeout), and deletes rows constantly, so dead
tuples pile up fast. With the default `autovacuum_vacuum_scale_factor` of `0.2`,
autovacuum only runs once dead tuples reach 20% of the table — by which point a
busy queue has bloated its heap and indexes, slowing every read. The archive
table grows mostly by append, so it benefits from a gentler-but-still-tightened
setting.

`tune_autovacuum` sets per-table storage parameters via `ALTER TABLE`, so
autovacuum runs far more often on *these specific tables* without touching
cluster-wide settings. It is **opt-in** — the gem never mutates storage
parameters unless you ask.

```ruby
# Tune an existing queue with PGMQ defaults:
#   queue table   (pgmq.q_<name>): scale_factor 0.01, threshold 50
#   archive table (pgmq.a_<name>): scale_factor 0.05, threshold 50
client.tune_autovacuum("orders")

# Override the queue setting and skip the archive table
client.tune_autovacuum("orders", scale_factor: 0.005, archive: false)

# Override every parameter
client.tune_autovacuum("orders",
  scale_factor: 0.01, threshold: 50,
  archive_scale_factor: 0.05, archive_threshold: 50)

# Or tune at creation time (true = defaults, or pass a Hash of the options above)
client.create("orders", tune_autovacuum: true)
client.create("orders", tune_autovacuum: { scale_factor: 0.005, archive: false })
client.create_unlogged("fast", tune_autovacuum: true)
client.create_partitioned("big", partition_interval: "daily",
  retention_interval: "7 days", tune_autovacuum: true)
```

> **Partitioned queues:** parameters are set on the partitioned *parent* table.
> PostgreSQL does not cascade storage parameters to existing partitions, so set
> them per-partition if you need to retune already-created partitions.

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

# Event-driven consumption via LISTEN/NOTIFY (more efficient than long-polling at low message rates)
# Step 1: enable server-side NOTIFY trigger once (idempotent)
client.enable_notify_insert("queue_name")

# Step 2: consumer loop — wake up on notification, then drain all available messages.
# With throttling (default 250ms), a burst of inserts fires only one NOTIFY.
# Always use read_batch after waking up to avoid leaving messages stranded.
loop do
  next unless client.wait_for_notify("queue_name", timeout: 5)

  msgs = client.read_batch("queue_name", vt: 30, qty: 10)
  msgs.each { |m| process(m); client.delete("queue_name", m.msg_id) }
end

# Pop (atomic read + delete)
msg = client.pop("queue_name")

# Pop batch (atomic read + delete for multiple messages)
messages = client.pop_batch("queue_name", 10)

# SQS-style grouped reading (throughput-first: drains oldest group before moving on)
# Messages grouped by first key in their JSON payload
messages = client.read_grouped("queue_name", vt: 30, qty: 10)

# Throughput-first grouped reading with long-polling
messages = client.read_grouped_with_poll("queue_name",
  vt: 30,
  qty: 10,
  max_poll_seconds: 5,
  poll_interval_ms: 100
)

# Grouped round-robin reading (fair processing across entities)
# Messages are grouped by the first key in their JSON payload
messages = client.read_grouped_rr("queue_name", vt: 30, qty: 10)

# Grouped round-robin with long-polling
messages = client.read_grouped_rr_with_poll("queue_name",
  vt: 30,
  qty: 10,
  max_poll_seconds: 5,
  poll_interval_ms: 100
)

# Read one message per FIFO group from the head of each group
# Groups are set via x-pgmq-group header (requires PGMQ v1.11.1+)
client.produce("queue_name", '{"job":"a"}', headers: '{"x-pgmq-group":"tenant_a"}')
client.produce("queue_name", '{"job":"b"}', headers: '{"x-pgmq-group":"tenant_b"}')
messages = client.read_grouped_head("queue_name", vt: 30, qty: 10)
# => one message from tenant_a and one from tenant_b
```

#### Grouped Reading

PGMQ provides three grouped-reading strategies for processing messages from multiple entities (users, tenants, orders). All three group by the **first key** in the JSON payload (except `read_grouped_head`, which uses the `x-pgmq-group` message header).

##### Throughput-First (`read_grouped` / `read_grouped_with_poll`)

Drains the oldest group completely before moving to the next. Best when maximising throughput matters more than fairness:

```ruby
# Queue: user_a has 3 messages, user_b has 1
messages = client.read_grouped("tasks", vt: 30, qty: 3)
# => [user_a_1, user_a_2, user_a_3]  - drains user_a first

# With long-polling (waits up to max_poll_seconds if queue is empty)
messages = client.read_grouped_with_poll("tasks",
  vt: 30,
  qty: 10,
  max_poll_seconds: 5,
  poll_interval_ms: 100
)
```

##### Round-Robin (`read_grouped_rr` / `read_grouped_rr_with_poll`)

Interleaves one message per group on each pass. Best for fairness - prevents any single entity from monopolising workers:

```ruby
# Queue: user_a: 5 messages, user_b: 2, user_c: 1
messages = client.read_grouped_rr("tasks", vt: 30, qty: 8)
# => [user_a_1, user_b_1, user_c_1, user_a_2, user_b_2, user_a_3, user_a_4, user_a_5]

# With long-polling
messages = client.read_grouped_rr_with_poll("tasks",
  vt: 30,
  qty: 10,
  max_poll_seconds: 5,
  poll_interval_ms: 100
)
```

**Message format for payload-based grouping** (used by `read_grouped` and `read_grouped_rr`):
```ruby
# user_id is first key - PGMQ uses it as the group identifier
client.produce("tasks", '{"user_id":"user_a","task":"process"}')
```

##### Head-of-Group (`read_grouped_head`) - PGMQ v1.11.1+

Returns the oldest visible message from each FIFO group, up to `qty` groups. Groups are identified by the `x-pgmq-group` key in the message **headers**. Messages without that header all share one implicit default group.

Useful for detecting head-of-line stalls or building per-group progress dashboards - one call surfaces the leading edge of every group simultaneously:

```ruby
# Produce with x-pgmq-group headers
client.produce("jobs", '{"task":"build"}', headers: '{"x-pgmq-group":"tenant_a"}')
client.produce("jobs", '{"task":"build"}', headers: '{"x-pgmq-group":"tenant_a"}')
client.produce("jobs", '{"task":"test"}',  headers: '{"x-pgmq-group":"tenant_b"}')

# Returns one message per group (oldest from each)
messages = client.read_grouped_head("jobs", vt: 30, qty: 100)
# => [tenant_a oldest msg, tenant_b oldest msg]

# Check for stuck groups
messages.each do |msg|
  group = JSON.parse(msg.headers)["x-pgmq-group"]
  puts "#{group} head enqueued at #{msg.enqueued_at}"
end
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

# Update visibility timeout with integer offset (seconds from now)
msg = client.set_vt("queue_name", msg_id, vt: 60)

# Update visibility timeout with absolute Time (PGMQ v1.11.0+)
future_time = Time.now + 300  # 5 minutes from now
msg = client.set_vt("queue_name", msg_id, vt: future_time)

# Batch update visibility timeout
updated_msgs = client.set_vt_batch("queue_name", [101, 102, 103], vt: 60)

# Batch update with absolute Time
updated_msgs = client.set_vt_batch("queue_name", [101, 102, 103], vt: Time.now + 120)

# Update visibility timeout across multiple queues
client.set_vt_multi({
  "orders" => [1, 2, 3],
  "notifications" => [5, 6]
}, vt: 120)

# Purge all messages
count = client.purge_queue("queue_name")

# Convert a standard queue's archive table to pg_partman-managed partitions (requires pg_partman)
# Useful for queues created with `create`/`create_unlogged` whose archives have grown large.
# Idempotent - safe to call if the archive is already partitioned or doesn't exist yet.
client.convert_archive_partitioned("queue_name")

# Custom partition/retention intervals (same syntax as create_partitioned)
client.convert_archive_partitioned("queue_name",
  partition_interval: "daily",
  retention_interval: "30 days"
)

# Enable PostgreSQL NOTIFY for a queue (for LISTEN-based consumers)
client.enable_notify_insert("queue_name", throttle_interval_ms: 250)

# Update the throttle interval without disabling/re-enabling the trigger
client.update_notify_insert("queue_name", throttle_interval_ms: 100)

# List all queues with NOTIFY enabled and their current throttle configuration
throttles = client.list_notify_insert_throttles
throttles.each do |t|
  puts "#{t.queue_name}: #{t.throttle_interval_ms}ms (last notified: #{t.last_notified_at})"
end

# Disable notifications
client.disable_notify_insert("queue_name")

# Block until a NOTIFY arrives on the queue's channel (or timeout expires)
# Returns the payload string on notification, nil on timeout
client.wait_for_notify("queue_name", timeout: 5)

# Block form — inspect notification metadata
client.wait_for_notify("queue_name", timeout: 5) do |channel, pid, payload|
  puts "Notified on #{channel} by backend #{pid}"
end
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

### Topic Routing (AMQP-like Patterns)

PGMQ v1.11.0+ supports AMQP-style topic routing, allowing messages to be delivered to multiple queues based on pattern matching.

#### Topic Patterns

Topic patterns support wildcards:
- `*` matches exactly one word (e.g., `orders.*` matches `orders.new` but not `orders.new.priority`)
- `#` matches zero or more words (e.g., `orders.#` matches `orders`, `orders.new`, and `orders.new.priority`)

```ruby
# Create queues for different purposes
client.create("new_orders")
client.create("order_updates")
client.create("all_orders")
client.create("audit_log")

# Bind topic patterns to queues
client.bind_topic("orders.new", "new_orders")        # Exact match
client.bind_topic("orders.update", "order_updates")  # Exact match
client.bind_topic("orders.*", "all_orders")          # Single-word wildcard
client.bind_topic("#", "audit_log")                  # Catch-all

# Send messages via routing key
# Message is delivered to ALL queues with matching patterns
count = client.produce_topic("orders.new", '{"order_id":123}')
# => 3 (delivered to: new_orders, all_orders, audit_log)

count = client.produce_topic("orders.update", '{"order_id":123,"status":"shipped"}')
# => 3 (delivered to: order_updates, all_orders, audit_log)

# Send with headers and delay
count = client.produce_topic("orders.new.priority",
  '{"order_id":456}',
  headers: '{"trace_id":"abc123"}',
  delay: 0
)

# Batch send via topic routing
results = client.produce_batch_topic("orders.new", [
  '{"order_id":1}',
  '{"order_id":2}',
  '{"order_id":3}'
])
# => [{ queue_name: "new_orders", msg_id: "1" }, ...]

# List all topic bindings
bindings = client.list_topic_bindings
bindings.each do |b|
  puts "#{b[:pattern]} -> #{b[:queue_name]}"
end

# List bindings for specific queue
bindings = client.list_topic_bindings(queue_name: "all_orders")

# Test which queues a routing key would match (for debugging)
matches = client.test_routing("orders.new.priority")
# => [{ pattern: "orders.#", queue_name: "all_orders" }, ...]

# Validate routing keys and patterns
client.validate_routing_key("orders.new.priority")  # => true
client.validate_routing_key("orders.*")             # => false (wildcards not allowed in keys)
client.validate_topic_pattern("orders.*")           # => true
client.validate_topic_pattern("orders.#")           # => true

# Remove bindings when done
client.unbind_topic("orders.new", "new_orders")
client.unbind_topic("orders.*", "all_orders")
```

**Use Cases:**
- **Event broadcasting**: Send events to multiple consumers based on event type
- **Multi-tenant routing**: Route messages to tenant-specific queues
- **Log aggregation**: Capture all messages in an audit queue while routing to specific handlers
- **Fan-out patterns**: Deliver one message to multiple processing pipelines

## Message Object

PGMQ-Ruby is a **low-level transport library** - it returns raw values from PostgreSQL without any transformation. You are responsible for parsing JSON and type conversion.

```ruby
msg = client.read("queue", vt: 30)

# All values are strings as returned by PostgreSQL
msg.msg_id          # => "123" (String, not Integer)
msg.id              # => "123" (alias for msg_id)
msg.read_ct         # => "1" (String, not Integer)
msg.enqueued_at     # => "2025-01-15 10:30:00+00" (String, not Time)
msg.last_read_at    # => "2025-01-15 10:30:15+00" (String, or nil if never read)
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
last_read = Time.parse(msg.last_read_at) if msg.last_read_at  # => Time or nil
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
bundle exec rake test

# Run all integration specs
bin/integrations

# Run a specific integration spec
bin/integrations spec/integration/basic_produce_consume_spec.rb

# Run console
bundle exec bin/console
```

## Author

Maintained by [Maciej Mensfeld](https://github.com/mensfeld)

Also check out [Karafka](https://karafka.io) - High-performance Apache Kafka framework for Ruby.
