# frozen_string_literal: true

module PGMQ
  class Client
    # Queue maintenance operations
    #
    # This module handles queue maintenance tasks such as purging messages and detaching archive tables.
    module Maintenance
      # Purges all messages from a queue
      #
      # @param queue_name [String] name of the queue
      # @return [Integer] number of messages purged
      #
      # @example
      #   count = client.purge_queue("old_queue")
      #   puts "Purged #{count} messages"
      def purge_queue(queue_name)
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params("SELECT pgmq.purge_queue($1::text)", [queue_name])
        end

        result[0]["purge_queue"]
      end

      # Enables PostgreSQL NOTIFY when messages are inserted into a queue
      #
      # When enabled, PostgreSQL will send a NOTIFY event on message insert, allowing clients to use LISTEN instead of
      # polling. The throttle interval prevents notification storms during high-volume inserts.
      #
      # @param queue_name [String] name of the queue
      # @param throttle_interval_ms [Integer] minimum ms between notifications (default: 250)
      # @return [void]
      #
      # @example Enable with default throttle (250ms)
      #   client.enable_notify_insert("orders")
      #
      # @example Enable with custom throttle (1 second)
      #   client.enable_notify_insert("orders", throttle_interval_ms: 1000)
      #
      # @example Disable throttling (notify on every insert)
      #   client.enable_notify_insert("orders", throttle_interval_ms: 0)
      def enable_notify_insert(queue_name, throttle_interval_ms: 250)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          conn.exec_params(
            "SELECT pgmq.enable_notify_insert($1::text, $2::integer)",
            [queue_name, throttle_interval_ms]
          )
        end

        nil
      end

      # Converts a standard queue's archive table to a pg_partman-managed partitioned table
      #
      # Provides a migration path for queues originally created with `create` or `create_unlogged` whose archive tables
      # have grown large enough to benefit from partitioning. The queue message table is not affected - only the archive
      # table (`pgmq.a_<queue_name>`) is converted.
      #
      # The operation renames the existing archive table, creates a new partitioned table with the same schema, and
      # hands it over to pg_partman for lifecycle management. If the archive table is already partitioned the function
      # returns without error (idempotent). If the archive table does not exist it also returns without error.
      #
      # @note Requires the `pg_partman` PostgreSQL extension.
      #
      # @param queue_name [String] name of the queue whose archive table to convert
      # @param partition_interval [String] partition interval passed to pg_partman (default: "10000" rows or a time
      #   expression such as "daily" / "1 month")
      # @param retention_interval [String] retention interval passed to pg_partman (default: "100000")
      # @param leading_partition [Integer] number of leading partitions pg_partman should pre-create (default: 10)
      # @return [void]
      # @raise [PGMQ::Errors::InvalidQueueNameError] if queue name is invalid
      # @raise [PGMQ::Errors::ConnectionError] if the database operation fails (e.g. pg_partman not installed)
      #
      # @example Convert with default partitioning (row-count based)
      #   client.convert_archive_partitioned("orders")
      #
      # @example Convert with time-based daily partitioning
      #   client.convert_archive_partitioned("orders",
      #     partition_interval: "daily",
      #     retention_interval: "30 days"
      #   )
      def convert_archive_partitioned(
        queue_name,
        partition_interval: "10000",
        retention_interval: "100000",
        leading_partition: 10
      )
        validate_queue_name!(queue_name)

        with_connection do |conn|
          conn.exec_params(
            "SELECT pgmq.convert_archive_partitioned($1::text, $2::text, $3::text, $4::integer)",
            [queue_name, partition_interval, retention_interval, leading_partition]
          )
        end

        nil
      end

      # Disables PostgreSQL NOTIFY for a queue
      #
      # @param queue_name [String] name of the queue
      # @return [void]
      #
      # @example
      #   client.disable_notify_insert("orders")
      def disable_notify_insert(queue_name)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          conn.exec_params("SELECT pgmq.disable_notify_insert($1::text)", [queue_name])
        end

        nil
      end
    end
  end
end
