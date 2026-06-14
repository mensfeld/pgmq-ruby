# frozen_string_literal: true

module PGMQ
  class Client
    # Queue management operations (create, drop, list)
    #
    # This module handles all queue lifecycle operations including creating queues (standard, partitioned, unlogged),
    # dropping queues, and listing existing queues.
    module QueueManagement
      # Creates a new queue
      #
      # @param queue_name [String] name of the queue to create
      # @param tune_autovacuum [Boolean, Hash] when truthy, tune autovacuum on the new queue's tables after creation.
      #   Pass +true+ for PGMQ-tuned defaults, or a Hash of options forwarded to {Autovacuum#tune_autovacuum}
      #   (e.g. +{scale_factor: 0.005, archive: false}+). Defaults to +false+ (no change).
      # @return [Boolean] true if queue was created, false if it already existed
      # @raise [PGMQ::Errors::InvalidQueueNameError] if queue name is invalid
      # @raise [PGMQ::Errors::ConnectionError] if database operation fails
      #
      # @example
      #   client.create("orders")  # => true (created)
      #   client.create("orders")  # => false (already exists)
      #
      # @example Create with tuned autovacuum
      #   client.create("orders", tune_autovacuum: true)
      def create(queue_name, tune_autovacuum: false)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          existed = queue_exists?(conn, queue_name)
          conn.exec_params("SELECT pgmq.create($1::text)", [queue_name])
          apply_tune_autovacuum_option(conn, queue_name, tune_autovacuum)
          !existed
        end
      end

      # Creates a partitioned queue
      #
      # Requires pg_partman extension to be installed
      #
      # @param queue_name [String] name of the queue
      # @param partition_interval [String] partition interval (e.g., "daily", "10000")
      # @param retention_interval [String] retention interval (e.g., "7 days", "100000")
      # @param tune_autovacuum [Boolean, Hash] when truthy, tune autovacuum on the new queue's tables after creation.
      #   Pass +true+ for PGMQ-tuned defaults, or a Hash forwarded to {Autovacuum#tune_autovacuum}. Defaults to
      #   +false+. Note: storage parameters are set on the partitioned parent and do not cascade to partitions.
      # @return [Boolean] true if queue was created, false if it already existed
      #
      # @example
      #   client.create_partitioned("big_queue",
      #     partition_interval: "daily",
      #     retention_interval: "7 days"
      #   )  # => true
      def create_partitioned(
        queue_name,
        partition_interval: "10000",
        retention_interval: "100000",
        tune_autovacuum: false
      )
        validate_queue_name!(queue_name)

        with_connection do |conn|
          existed = queue_exists?(conn, queue_name)
          conn.exec_params(
            "SELECT pgmq.create_partitioned($1::text, $2::text, $3::text)",
            [queue_name, partition_interval, retention_interval]
          )
          apply_tune_autovacuum_option(conn, queue_name, tune_autovacuum)
          !existed
        end
      end

      # Creates an unlogged queue for higher throughput (no crash recovery)
      #
      # @param queue_name [String] name of the queue
      # @param tune_autovacuum [Boolean, Hash] when truthy, tune autovacuum on the new queue's tables after creation.
      #   Pass +true+ for PGMQ-tuned defaults, or a Hash forwarded to {Autovacuum#tune_autovacuum}. Defaults to
      #   +false+.
      # @return [Boolean] true if queue was created, false if it already existed
      #
      # @example
      #   client.create_unlogged("fast_queue")  # => true
      def create_unlogged(queue_name, tune_autovacuum: false)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          existed = queue_exists?(conn, queue_name)
          conn.exec_params("SELECT pgmq.create_unlogged($1::text)", [queue_name])
          apply_tune_autovacuum_option(conn, queue_name, tune_autovacuum)
          !existed
        end
      end

      # Drops a queue and its archive table
      #
      # @param queue_name [String] name of the queue to drop
      # @return [Boolean] true if queue was dropped
      #
      # @example
      #   client.drop_queue("old_queue")
      def drop_queue(queue_name)
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params("SELECT pgmq.drop_queue($1::text)", [queue_name])
        end

        return false if result.ntuples.zero?

        result[0]["drop_queue"] == "t"
      end

      # Creates the FIFO index on a queue's table required for grouped reads
      #
      # Grouped read operations (`read_grouped`, `read_grouped_rr`, `read_grouped_head`) rely on this index for correct
      # ordering and acceptable query performance. Without it, grouped reads will work but may be slow or return
      # incorrect ordering at scale. The operation is idempotent - calling it on a queue that already has the index is
      # safe.
      #
      # @param queue_name [String] name of the queue
      # @return [void]
      # @raise [PGMQ::Errors::InvalidQueueNameError] if queue name is invalid
      # @raise [PGMQ::Errors::ConnectionError] if database operation fails
      #
      # @example
      #   client.create("tasks")
      #   client.create_fifo_index("tasks")
      def create_fifo_index(queue_name)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          conn.exec_params("SELECT pgmq.create_fifo_index($1::text)", [queue_name])
        end

        nil
      end

      # Creates FIFO indexes on all existing queues
      #
      # Convenience wrapper that calls `create_fifo_index` for every queue registered in `pgmq.meta`. Useful for
      # one-time migrations when adding grouped reads to an existing deployment. The operation is idempotent.
      #
      # @return [void]
      # @raise [PGMQ::Errors::ConnectionError] if database operation fails
      #
      # @example Migrate an existing deployment to use grouped reads
      #   client.create_fifo_indexes_all
      def create_fifo_indexes_all
        with_connection do |conn|
          conn.exec("SELECT pgmq.create_fifo_indexes_all()")
        end

        nil
      end

      # Lists all queues
      #
      # @return [Array<PGMQ::QueueMetadata>] array of queue metadata objects
      #
      # @example
      #   queues = client.list_queues
      #   queues.each { |q| puts q.queue_name }
      def list_queues
        result = with_connection do |conn|
          conn.exec("SELECT * FROM pgmq.list_queues()")
        end

        result.map { |row| QueueMetadata.new(row) }
      end

      private

      # Checks if a queue exists in the pgmq.meta table
      #
      # @param conn [PG::Connection] database connection
      # @param queue_name [String] name of the queue to check
      # @return [Boolean] true if queue exists, false otherwise
      def queue_exists?(conn, queue_name)
        result = conn.exec_params(
          "SELECT 1 FROM pgmq.meta WHERE queue_name = $1 LIMIT 1",
          [queue_name]
        )
        result.ntuples.positive?
      end

      # Applies the +tune_autovacuum:+ creation option on the create connection.
      #
      # Accepts the same shapes the create methods document: +false+/+nil+ (no-op), +true+ (PGMQ-tuned defaults), or a
      # Hash of overrides. Delegates resolution and the ALTER TABLE statements to {Autovacuum#tune_autovacuum_on}, the
      # single source of truth shared with {Autovacuum#tune_autovacuum}, so defaults and the archive-skip rule cannot
      # drift between the two paths. Runs on the connection that just created the queue, so the ALTER TABLE shares that
      # checkout rather than acquiring a second one.
      #
      # @param conn [PG::Connection] the connection the queue was created on
      # @param queue_name [String] name of the queue
      # @param option [Boolean, Hash] the tune_autovacuum option as passed to the create method
      # @return [void]
      def apply_tune_autovacuum_option(conn, queue_name, option)
        return unless option

        tune_autovacuum_on(conn, queue_name, option.is_a?(Hash) ? option : {})
      end
    end
  end
end
