# frozen_string_literal: true

module PGMQ
  class Client
    # Queue metrics and monitoring
    #
    # This module handles retrieving queue metrics such as queue length,
    # message age, and total message counts.
    module Metrics
      # Gets metrics for a specific queue
      #
      # @param queue_name [String] name of the queue
      # @return [PGMQ::Metrics] metrics object
      #
      # @example
      #   metrics = client.metrics("orders")
      #   puts "Queue length: #{metrics.queue_length}"
      #   puts "Oldest message: #{metrics.oldest_msg_age_sec}s"
      def metrics(queue_name)
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params('SELECT * FROM pgmq.metrics($1::text)', [queue_name])
        end

        return nil if result.ntuples.zero?

        PGMQ::Metrics.new(result[0])
      end

      # Gets metrics for all queues
      #
      # @return [Array<PGMQ::Metrics>] array of metrics objects
      #
      # @example
      #   all_metrics = client.metrics_all
      #   all_metrics.each do |m|
      #     puts "#{m.queue_name}: #{m.queue_length} messages"
      #   end
      def metrics_all
        result = with_connection do |conn|
          conn.exec('SELECT * FROM pgmq.metrics_all()')
        end

        result.map { |row| PGMQ::Metrics.new(row) }
      end
    end
  end
end
