# frozen_string_literal: true

module PGMQ
  module Contracts
    # Validates the messages and headers arrays of a batch produce
    class Batch < Contractable::Contract
      self.error_messages = Contracts.error_messages("batch")

      required(:messages) { |messages| messages.is_a?(Array) }
      optional(:headers) { |headers| headers.nil? || headers.is_a?(Array) }

      virtual do |data, errors|
        next true unless errors.empty?
        next true if data[:headers].nil?
        next true if data[:headers].length == data[:messages].length

        [[[:headers], :length]]
      end
    end
  end
end
