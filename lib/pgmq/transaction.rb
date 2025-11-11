# frozen_string_literal: true

module PGMQ
  # Low-level transaction support for PGMQ operations
  #
  # Provides atomic execution of PGMQ operations within PostgreSQL transactions.
  # Transactions are a database primitive - this is a thin wrapper around
  # PostgreSQL's native transaction support.
  #
  # This is analogous to rdkafka-ruby providing Kafka transaction support -
  # it's a protocol/database feature, not a framework abstraction.
  #
  # @example Atomic multi-queue operations
  #   client.transaction do |txn|
  #     txn.send("orders", { order_id: 123 })
  #     txn.send("notifications", { type: "order_created" })
  #   end
  #
  # @example Automatic rollback on error
  #   client.transaction do |txn|
  #     txn.send("orders", { order_id: 123 })
  #     raise "Error"  # Both operations rolled back
  #   end
  module Transaction
    # Executes PGMQ operations atomically within a database transaction
    #
    # Obtains a connection from the pool, starts a transaction, and yields
    # a client that uses that transaction for all operations.
    #
    # @yield [PGMQ::Client] transactional client using the same connection
    # @return [Object] result of the block
    # @raise [PGMQ::Errors::ConnectionError] if transaction fails
    #
    # @example
    #   client.transaction do |txn|
    #     msg_id = txn.send("queue", { data: "test" })
    #     txn.delete("queue", msg_id)
    #   end
    def transaction
      @connection.with_connection do |conn|
        conn.transaction do
          yield TransactionalClient.new(self, conn)
        end
      end
    rescue PG::Error, StandardError => e
      raise PGMQ::Errors::ConnectionError, "Transaction failed: #{e.message}"
    end

    # Minimal wrapper that ensures all operations use the transaction connection
    #
    # @private
    class TransactionalClient
      # @param parent [PGMQ::Client] parent client instance
      # @param conn [PG::Connection] transaction connection
      def initialize(parent, conn)
        @parent = parent
        @conn = conn
      end

      # Forward all method calls to parent, but use our transaction connection
      # @param method [Symbol] method name
      # @return [Object] result of method call
      def method_missing(method, ...)
        @parent.respond_to?(method, true) ? @parent.__send__(method, ...) : super
      end

      # Override Object#send to call parent's send method
      # @param queue_name [String] queue name
      # @param message [String] message as JSON string
      # @param delay [Integer] delay in seconds
      # @return [String] message ID
      def send(queue_name, message, delay: 0)
        @parent.send(queue_name, message, delay: delay)
      end

      # Check if method exists on parent
      # @param method [Symbol] method name
      # @param include_private [Boolean] include private methods
      # @return [Boolean] true if method exists
      def respond_to_missing?(method, include_private = false)
        @parent.respond_to?(method, include_private) || super
      end

      # Inject our transaction connection instead of using the pool
      def with_connection
        yield @conn
      end

      # Expose parent's connection (for tests/inspection)
      def connection
        @parent.connection
      end
    end
  end
end
