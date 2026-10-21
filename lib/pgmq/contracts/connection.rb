# frozen_string_literal: true

module PGMQ
  module Contracts
    # Validates the configuration of {PGMQ::Connection}
    #
    # Each key is optional, so one contract serves the constructor and the class-level settings.
    class Connection < Contractable::Contract
      self.error_messages = Contracts.error_messages("connection")

      optional(:conn_params) do |conn_params|
        next :empty if conn_params.nil?
        next true if conn_params.respond_to?(:call) || conn_params.is_a?(String)

        conn_params.is_a?(Hash) && !conn_params.empty?
      end

      optional(:reconnectable_error_patterns) do |patterns|
        Array(patterns).all? { |pattern| pattern.is_a?(String) || pattern.is_a?(Regexp) }
      end

      optional(:reconnectable_error_classes) do |classes|
        Array(classes).all? { |klass| klass.is_a?(Class) && klass <= Exception }
      end
    end
  end
end
