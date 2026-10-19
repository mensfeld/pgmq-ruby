# frozen_string_literal: true

module PGMQ
  # PGMQ errors namespace
  module Errors
    # Base error class for all PGMQ errors
    class BaseError < StandardError; end

    # Raised when a database call fails, not only when the connection to PostgreSQL fails or is lost.
    # {PGMQ::Connection#with_connection} wraps every `PG::Error` (for example a missing queue, invalid JSON or an
    # invalid integer) in this class, with the message "Database connection error: ...". It also raises it for
    # connection pool timeouts and a closed pool. {PGMQ::Client#transaction} raises it when the transaction fails.
    # The original error is in `#cause`.
    class ConnectionError < BaseError; end

    # Raised when a queue operation is attempted on a non-existent queue
    #
    # @note Not raised by PGMQ-Ruby. A database error for a missing queue raises {ConnectionError}.
    class QueueNotFoundError < BaseError; end

    # Raised when a message cannot be found
    #
    # @note Not raised by PGMQ-Ruby.
    class MessageNotFoundError < BaseError; end

    # Raised when message serialization fails
    #
    # @note Not raised by PGMQ-Ruby.
    class SerializationError < BaseError; end

    # Raised when message deserialization fails
    #
    # @note Not raised by PGMQ-Ruby.
    class DeserializationError < BaseError; end

    # Raised when configuration is invalid
    class ConfigurationError < BaseError; end

    # Raised when an invalid queue name is provided
    class InvalidQueueNameError < BaseError; end

    # Raised when a transactional client is used outside its transaction (another thread/fiber, or after the block)
    class TransactionError < BaseError; end
  end
end
