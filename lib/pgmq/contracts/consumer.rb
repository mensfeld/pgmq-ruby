# frozen_string_literal: true

module PGMQ
  module Contracts
    # Validates the options of {PGMQ::Consumer}
    #
    # Queue names themselves are validated by {PGMQ::QueueName}.
    class Consumer < Contractable::Contract
      # Actions that can be taken on a processed message
      ON_SUCCESS_ACTIONS = %i[delete archive].freeze

      self.error_messages = Contracts.error_messages("consumer")

      required(:queue_names) do |queue_names|
        next :empty if queue_names.empty?
        next :too_many if queue_names.size > MultiQueue::MAX_QUEUES
        # pgmq folds queue names to lower case, so names differing only in case refer to the same queue
        next :unique unless queue_names.map { |name| name.to_s.downcase }.uniq.size == queue_names.size

        true
      end

      required(:on_success) { |on_success| ON_SUCCESS_ACTIONS.include?(on_success) }

      %i[vt batch_size max_poll_seconds poll_interval_ms].each do |key|
        required(key) { |value| value.is_a?(Integer) && value.positive? }
      end

      optional(:max_attempts) do |max_attempts|
        max_attempts.nil? || (max_attempts.is_a?(Integer) && max_attempts.positive?)
      end

      # Every read raises read_ct, so with larger batches the messages released after a failure would have their
      # unprocessed read counted as a failed attempt
      virtual do |data, errors|
        next true unless errors.empty?
        next true if data[:max_attempts].nil? || data[:batch_size] == 1

        [[[:max_attempts], :batch_size]]
      end
    end
  end
end
