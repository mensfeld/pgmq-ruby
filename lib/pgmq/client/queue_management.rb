# frozen_string_literal: true

module PGMQ
  class Client
    # Queue management operations (create, drop, list)
    #
    # This module handles all queue lifecycle operations including creating queues
    # (standard, partitioned, unlogged), dropping queues, and listing existing queues.
    module QueueManagement
      # Creates a new queue
      #
      # @param queue_name [String] name of the queue to create
      # @return [Boolean] true if queue was created, false if it already existed
      # @raise [PGMQ::Errors::InvalidQueueNameError] if queue name is invalid
      # @raise [PGMQ::Errors::ConnectionError] if database operation fails
      #
      # @example
      #   client.create("orders")  # => true (created)
      #   client.create("orders")  # => false (already exists)
      def create(queue_name)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          existed = queue_exists?(conn, queue_name)
          conn.exec_params("SELECT pgmq.create($1::text)", [queue_name])
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
        retention_interval: "100000"
      )
        validate_queue_name!(queue_name)

        with_connection do |conn|
          existed = queue_exists?(conn, queue_name)
          conn.exec_params(
            "SELECT pgmq.create_partitioned($1::text, $2::text, $3::text)",
            [queue_name, partition_interval, retention_interval]
          )
          !existed
        end
      end

      # Creates an unlogged queue for higher throughput (no crash recovery)
      #
      # @param queue_name [String] name of the queue
      # @return [Boolean] true if queue was created, false if it already existed
      #
      # @example
      #   client.create_unlogged("fast_queue")  # => true
      def create_unlogged(queue_name)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          existed = queue_exists?(conn, queue_name)
          conn.exec_params("SELECT pgmq.create_unlogged($1::text)", [queue_name])
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
    end
  end
end
