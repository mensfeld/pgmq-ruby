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
          conn.exec_params('SELECT pgmq.purge_queue($1::text)', [queue_name])
        end

        result[0]['purge_queue']
      end

      # Detaches the archive table from PGMQ management
      #
      # @param queue_name [String] name of the queue
      # @return [void]
      #
      # @example
      #   client.detach_archive("orders")
      def detach_archive(queue_name)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          conn.exec_params('SELECT pgmq.detach_archive($1::text)', [queue_name])
        end

        nil
      end
    end
  end
end
