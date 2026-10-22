# frozen_string_literal: true

module PGMQ
  module Contracts
    # Validates the storage settings of {PGMQ::Client::Autovacuum#tune_autovacuum}
    #
    # Parameter names cannot be bound, so only the known names may reach the ALTER TABLE statement, and only values
    # that coerce to a number.
    class Autovacuum < Contractable::Contract
      # Storage parameters that can be set: the union of the queue and archive default keys
      STORAGE_PARAMS = (
        Client::Autovacuum::DEFAULT_QUEUE_SETTINGS.keys + Client::Autovacuum::DEFAULT_ARCHIVE_SETTINGS.keys
      ).uniq.freeze

      # Storage parameters whose values are Floats; everything else is an Integer
      FLOAT_SETTINGS = %i[
        autovacuum_vacuum_scale_factor
        autovacuum_analyze_scale_factor
      ].freeze

      self.error_messages = Contracts.error_messages("autovacuum")

      %i[queue_settings archive_settings].each do |key|
        required(key) do |settings|
          next :format unless (settings.keys - STORAGE_PARAMS).empty?
          next :values unless settings.all? { |name, value| numeric_setting?(name, value) }

          true
        end
      end

      class << self
        # @param name [Symbol] storage parameter name
        # @param value [Object] value to check
        # @return [Boolean] true when the value coerces to the Float or Integer the parameter needs
        def numeric_setting?(name, value)
          if FLOAT_SETTINGS.include?(name)
            !Float(value, exception: false).nil?
          else
            !Integer(value, exception: false).nil?
          end
        end
      end
    end
  end
end
