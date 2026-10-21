# frozen_string_literal: true

module PGMQ
  module Contracts
    # Validates the arguments of operations that span several queues
    #
    # Each key is optional, so one contract serves every multi-queue operation. Queue names themselves are validated
    # by {PGMQ::QueueName}.
    class MultiQueue < Contractable::Contract
      # Maximum number of queues in one multi-queue operation
      MAX_QUEUES = 50

      self.error_messages = Contracts.error_messages("multi_queue")

      optional(:queue_names) do |queue_names|
        next :format unless queue_names.is_a?(Array)
        next :empty if queue_names.empty?
        next :too_many if queue_names.size > MAX_QUEUES

        true
      end

      %i[deletions archives updates].each do |key|
        optional(key) { |value| value.is_a?(Hash) }
      end
    end
  end
end
