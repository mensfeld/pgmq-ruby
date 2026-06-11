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
      # The operation renames the existing archive table to `pgmq.a_<queue_name>_old`, creates a new partitioned table
      # with the same schema, and hands it over to pg_partman for lifecycle management. **Existing archived rows are
      # left in the `_old` table and must be migrated manually** if visibility in the new partitioned archive is needed.
      # If the archive table is already partitioned the function returns without error (idempotent). If the archive
      # table does not exist it also returns without error.
      #
      # @note Requires the `pg_partman` PostgreSQL extension. If pg_partman is not installed and the archive table
      #   exists, the call raises `PGMQ::Errors::ConnectionError`. If the archive table does not exist the call
      #   succeeds (returns nil) without touching pg_partman, so no extension is needed in that case.
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

      # Blocks until a PostgreSQL NOTIFY arrives on the queue's channel or the timeout expires.
      #
      # PGMQ sends notifications on the channel `pgmq.q_<queue_name>.INSERT` whenever a message is inserted (requires
      # {#enable_notify_insert} to be called first). This method issues `LISTEN`, waits for a notification via the `pg`
      # gem's `wait_for_notify`, then issues `UNLISTEN` before returning the connection to the pool.
      #
      # Compared with {PGMQ::Client::Consumer#read_with_poll}, which holds the connection inside a PL/pgSQL loop for
      # the full poll window, `wait_for_notify` releases the connection the moment a notification arrives (or the
      # timeout expires). This makes it more efficient under low message rates where the poll window would otherwise
      # burn idle time.
      #
      # The optional block receives `channel`, `backend_pid`, and `payload` when a notification arrives.
      # The return value mirrors `PG::Connection#wait_for_notify`: the notification payload string on success,
      # or `nil` on timeout.
      #
      # @note Orchestration (retry loop, reconnect-on-drop, graceful shutdown) is the caller's responsibility.
      #   This method is a thin primitive — it listens once, waits, and returns.
      #
      # @param queue_name [String] name of the queue (must have notifications enabled via {#enable_notify_insert})
      # @param timeout [Numeric, nil] seconds to wait; `nil` blocks indefinitely
      # @return [String, nil] notification payload, or `nil` if the timeout expired
      #
      # @example Basic usage (wake up when a message arrives, then read it)
      #   client.enable_notify_insert("orders")
      #
      #   loop do
      #     next unless client.wait_for_notify("orders", timeout: 5)
      #     msg = client.read("orders", vt: 30)
      #     process(msg) if msg
      #   end
      #
      # @example Block form (inspect notification metadata)
      #   client.wait_for_notify("orders", timeout: 5) do |channel, pid, payload|
      #     puts "Notified on #{channel} by backend #{pid}"
      #   end
      def wait_for_notify(queue_name, timeout: nil)
        validate_queue_name!(queue_name)
        # PGMQ trigger fires pg_notify('pgmq.q_<queue>.INSERT', NULL)
        channel = "pgmq.q_#{queue_name}.INSERT"

        with_connection do |conn|
          conn.exec("LISTEN \"#{channel}\"")
          begin
            conn.wait_for_notify(timeout) do |ch, pid, payload|
              yield ch, pid, payload if block_given?
            end
          ensure
            conn.exec("UNLISTEN \"#{channel}\"")
          end
        end
      end
    end
  end
end
