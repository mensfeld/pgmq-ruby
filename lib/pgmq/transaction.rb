# frozen_string_literal: true

module PGMQ
  # Low-level transaction support for PGMQ operations
  #
  # Provides atomic execution of PGMQ operations within PostgreSQL transactions. Transactions are a database primitive -
  # this is a thin wrapper around PostgreSQL's native transaction support.
  #
  # This is analogous to rdkafka-ruby providing Kafka transaction support - it's a protocol/database feature, not a
  # framework abstraction.
  #
  # @example Atomic multi-queue operations
  #   client.transaction do |txn|
  #     txn.produce("orders", '{"order_id":123}')
  #     txn.produce("notifications", '{"type":"order_created"}')
  #   end
  #
  # @example Automatic rollback on error
  #   client.transaction do |txn|
  #     txn.produce("orders", '{"order_id":123}')
  #     raise "Error"  # Both operations rolled back
  #   end
  #
  # @note The transactional client is bound to the fiber (and thread) that opened the transaction and is valid only
  #   inside the block. Using it from another thread or fiber, or after the block returns, raises
  #   {PGMQ::Errors::TransactionError} instead of silently running the operation outside the transaction.
  module Transaction
    # Executes PGMQ operations atomically within a database transaction
    #
    # Obtains a connection from the pool, starts a transaction, and yields a client that uses that transaction for all
    # operations made from the calling thread/fiber while the block runs. The yielded client raises
    # {PGMQ::Errors::TransactionError} when used from another thread or fiber, or after the block has returned.
    #
    # @yield [PGMQ::Client] transactional client using the same connection
    # @return [Object] result of the block
    # @raise [PGMQ::Errors::TransactionError] if the transactional client was misused and the error was not rescued
    # @raise [PGMQ::Errors::ConnectionError] if transaction fails
    #
    # @example
    #   client.transaction do |txn|
    #     msg_id = txn.produce("queue", '{"data":"test"}')
    #     txn.delete("queue", msg_id)
    #   end
    def transaction
      @connection.with_connection do |conn|
        conn.transaction do
          txn = TransactionalClient.new(self, conn)

          begin
            yield txn
          ensure
            txn.finish!
          end
        end
      end
    rescue PGMQ::Errors::TransactionError
      raise
    rescue PG::Error, StandardError => e
      raise PGMQ::Errors::ConnectionError, "Transaction failed: #{e.message}"
    end

    # Minimal wrapper that ensures all operations use the transaction connection
    #
    # @private
    class TransactionalClient
      # @param parent [PGMQ::Client] parent client instance
      # @param conn [PG::Connection] transaction connection
      def initialize(
        parent,
        conn
      )
        @parent = parent
        @conn = conn
        @owner = Fiber.current
        @finished = false
      end

      # Forward all method calls to parent, but use our transaction connection
      # @param method [Symbol] method name
      # @return [Object] result of method call
      # @raise [PGMQ::Errors::TransactionError] if used off the owner fiber or after the transaction ended
      def method_missing(method, ...)
        return super unless @parent.respond_to?(method, true)

        ensure_usable!(method)
        @parent.__send__(method, ...)
      end

      # Check if method exists on parent
      # @param method [Symbol] method name
      # @param include_private [Boolean] include private methods
      # @return [Boolean] true if method exists
      def respond_to_missing?(
        method,
        include_private = false
      )
        @parent.respond_to?(method, include_private) || super
      end

      # Inject our transaction connection instead of using the pool
      # @raise [PGMQ::Errors::TransactionError] if used off the owner fiber or after the transaction ended
      def with_connection
        ensure_usable!(:with_connection)
        yield @conn
      end

      # Expose parent's connection (for tests/inspection)
      def connection
        @parent.connection
      end

      # Marks the transaction as ended so any further use raises
      # @return [void]
      def finish!
        @finished = true
      end

      private

      # Parent operations reach the transaction connection only through the pool's re-entrant checkout, which is keyed
      # on the current fiber. Another thread/fiber would get a different connection and commit outside the transaction.
      #
      # @param method [Symbol] method being called, for the error message
      # @raise [PGMQ::Errors::TransactionError] if used off the owner fiber or after the transaction ended
      def ensure_usable!(method)
        if @finished
          raise PGMQ::Errors::TransactionError, "Cannot call ##{method}: the transaction has already ended"
        end

        return if Fiber.current.equal?(@owner)

        raise PGMQ::Errors::TransactionError,
          "Cannot call ##{method} from another thread or fiber: the transactional client is bound to the fiber " \
          "that opened the transaction"
      end
    end
  end
end
