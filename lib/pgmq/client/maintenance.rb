# frozen_string_literal: true

module PGMQ
  class Client
    # Queue maintenance operations
    #
    # This module handles queue maintenance tasks such as purging messages
    # and detaching archive tables.
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
      # When enabled, PostgreSQL will send a NOTIFY event on message insert,
      # allowing clients to use LISTEN instead of polling. The throttle interval
      # prevents notification storms during high-volume inserts.
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
