# PGMQ-Ruby Development Guide

## Project Structure

```
pgmq-ruby/
├── lib/
│   ├── pgmq.rb                          # Main entry point
│   └── pgmq/
│       ├── client.rb                    # Main PGMQ client
│       ├── configuration.rb             # Configuration management
│       ├── connection.rb                # Database connection pooling
│       ├── errors.rb                    # Custom exception hierarchy
│       ├── message.rb                   # Message model
│       ├── metrics.rb                   # Queue metrics model
│       ├── queue_metadata.rb            # Queue metadata model
│       ├── version.rb                   # Gem version
│       └── serializers/
│           ├── base.rb                  # Base serializer class
│           └── json.rb                  # JSON serializer (default)
├── test/
│   ├── test_helper.rb                   # Minitest configuration + SimpleCov
│   ├── support/
│   │   └── database_helpers.rb          # Test helpers
│   └── lib/                             # Unit tests (minitest/spec)
│       ├── pgmq_test.rb
│       ├── pgmq/
│       │   ├── client_test.rb
│       │   ├── connection_test.rb
│       │   ├── message_test.rb
│       │   ├── metrics_test.rb
│       │   ├── queue_metadata_test.rb
│       │   ├── errors_test.rb
│       │   ├── transaction_test.rb
│       │   ├── version_test.rb
│       │   └── client/
│       │       ├── consumer_test.rb
│       │       ├── producer_test.rb
│       │       ├── multi_queue_test.rb
│       │       ├── message_lifecycle_test.rb
│       │       ├── queue_management_test.rb
│       │       ├── maintenance_test.rb
│       │       ├── metrics_test.rb
│       │       └── topics_test.rb
├── spec/
│   └── integration/                     # Integration examples (standalone scripts)
├── examples/                            # Usage examples
│   ├── basic_usage.rb
│   ├── worker_pattern.rb
│   └── rails_usage.rb
├── bin/
│   └── console                          # Interactive console
├── docker-compose.yml                   # PostgreSQL with PGMQ for testing
├── Gemfile                              # Dependencies
├── Rakefile                             # Rake tasks
├── pgmq-ruby.gemspec                    # Gem specification
├── README.md                            # User documentation
├── CHANGELOG.md                         # Version history
├── LICENSE                              # LGPL-3.0 license
```

## Setup

### Prerequisites

- Ruby 3.3 or higher
- PostgreSQL 14-18 with PGMQ extension
- Docker (optional, for running PostgreSQL with PGMQ)

### Installation

```bash
# Clone the repository
git clone https://github.com/mensfeld/pgmq-ruby.git
cd pgmq-ruby

# Install dependencies
bundle install

# Start PostgreSQL with PGMQ extension (using Docker)
docker compose up -d

# Wait for PostgreSQL to be ready
sleep 5
```

## Development Workflow

### Running Tests

```bash
# Run all tests
bundle exec rake test

# Run specific test file
bundle exec ruby -Ilib:test test/lib/pgmq/client_test.rb

# Run with coverage report
bundle exec rake test
# Coverage report will be in coverage/index.html
```

### Code Quality

```bash
# Run tests
bundle exec rake test
```

### Interactive Console

```bash
# Start interactive console with PGMQ client
bundle exec bin/console

# Inside console:
$client.create("test_queue")
$client.send("test_queue", { hello: "world" })
$client.read("test_queue", vt: 30)
```

### Running Examples

```bash
# Basic usage example
bundle exec ruby examples/basic_usage.rb

# Worker pattern (run as worker)
bundle exec ruby examples/worker_pattern.rb

# Worker pattern (run as producer)
bundle exec ruby examples/worker_pattern.rb producer

# Worker pattern (demo mode with test jobs)
bundle exec ruby examples/worker_pattern.rb demo

# Rails-like usage
bundle exec ruby examples/rails_usage.rb
```

## Testing

### Test Structure

- **Unit tests** (`test/lib/`): Minitest/spec tests for classes in isolation and with database
- **Integration tests** (`spec/integration/`): Standalone example scripts with real PostgreSQL

### Test Coverage

The project uses SimpleCov for code coverage tracking:

- Minimum coverage target: 80%
- Minimum coverage per file: 70%
- Coverage reports are generated automatically when running tests
- View coverage: `open coverage/index.html` (Mac) or `xdg-open coverage/index.html` (Linux)

### Running Tests Against PostgreSQL

Integration tests require a running PostgreSQL instance with PGMQ extension:

```bash
# Start PostgreSQL
docker compose up -d

# Run tests
bundle exec rake test

# Stop PostgreSQL
docker compose down
```

### Test Environment Variables

Override default database connection:

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=pgmq_test
export PG_USER=postgres
export PG_PASSWORD=postgres

bundle exec rake test
```

## Building the Gem

```bash
# Build gem
bundle exec rake build

# Install locally
bundle exec rake install

# Release to RubyGems (requires credentials)
bundle exec rake release
```

## Architecture

### Connection Management

- Uses `connection_pool` gem for thread-safe connection pooling
- Supports multiple connection strategies:
  - Connection strings (`postgres://...`)
  - Hash of parameters
  - Existing `PG::Connection` objects
  - Environment variables
- Auto-detects Rails/ActiveRecord connections (future)

### Serialization

- Pluggable serializer system
- JSON serializer included (default)
- Easy to add custom serializers (implement `PGMQ::Serializers::Base`)

### Error Handling

Custom exception hierarchy:
- `PGMQ::Errors::BaseError` - Base error
- `PGMQ::Errors::ConnectionError` - Connection failures
- `PGMQ::Errors::QueueNotFoundError` - Queue doesn't exist
- `PGMQ::Errors::MessageNotFoundError` - Message not found
- `PGMQ::Errors::SerializationError` - Serialization failures
- `PGMQ::Errors::ConfigurationError` - Invalid configuration
- `PGMQ::Errors::InvalidQueueNameError` - Invalid queue name

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-new-feature`)
3. Write tests for your changes
4. Implement your feature
5. Ensure tests pass: `bundle exec rake test`

6. Commit your changes (`git commit -am 'Add some feature'`)
7. Push to the branch (`git push origin feature/my-new-feature`)
8. Create a new Pull Request

### Code Style

- Follow Ruby style guide
- Write YARD documentation for public methods
- Keep methods small and focused
- Prefer composition over inheritance

### Testing Guidelines

- Write tests for all new features
- Maintain or improve code coverage
- Use descriptive test names
- Follow AAA pattern (Arrange, Act, Assert)
- Mock external dependencies in unit tests
- Use real database for integration tests


## Release Process

1. Update version in `lib/pgmq/version.rb`
2. Update `CHANGELOG.md` with changes
3. Commit changes: `git commit -am "Release v0.X.0"`
4. Create git tag: `git tag v0.X.0`
5. Push commits and tags: `git push && git push --tags`
6. Build and release gem: `bundle exec rake release`

## Roadmap

### v0.1.0 (Current - Placeholder)
- [x] Reserve gem name on RubyGems
- [x] Basic project structure

### v0.5.0 (Core Features)
- [x] Queue management (create, drop, list)
- [x] Message operations (send, read, delete)
- [x] Batch operations
- [x] Archive support
- [x] Metrics
- [x] Connection pooling
- [x] Comprehensive tests
- [x] Documentation

### Future: pgmq-framework gem (Higher-level features)
- [ ] Rails integration (Railtie, ActiveJob adapter)
- [ ] Rake tasks for queue management
- [ ] Rails generators
- [ ] Job processor framework
- [ ] Worker process management
- [ ] Retry strategies with exponential backoff
- [ ] Instrumentation and observability
- [ ] Performance benchmarks
- [ ] Security audit

### v1.1.0 (Advanced Features)
- [x] Partitioned queue support
- [x] Unlogged queue support
- [ ] GlobalID support (for ActiveRecord objects)
- [ ] Dead letter queue pattern

## Troubleshooting

### Tests Failing

```bash
# Ensure PostgreSQL is running
docker compose ps

# Check PostgreSQL logs
docker compose logs postgres

# Restart PostgreSQL
docker compose restart postgres

# Reset test database
docker compose down -v
docker compose up -d
```

### Connection Issues

```bash
# Test connection manually
psql "postgres://postgres:postgres@localhost:5432/pgmq_test"

# Check if PGMQ extension is installed
psql -c "SELECT * FROM pg_extension WHERE extname='pgmq';" \
  "postgres://postgres:postgres@localhost:5432/pgmq_test"
```


## Resources

- [PGMQ Documentation](https://github.com/pgmq/pgmq)
- [Ruby Style Guide](https://rubystyle.guide/)
- [Minitest](https://github.com/minitest/minitest)

## License

LGPL-3.0 - See LICENSE file for details
