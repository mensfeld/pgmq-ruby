# frozen_string_literal: true

module PGMQ
  module Contracts
    # Validates the storage settings of {PGMQ::Client::Autovacuum#tune_autovacuum}
    #
    # Parameter names cannot be bound, so only the known names may reach the ALTER TABLE statement.
    class Autovacuum < Contractable::Contract
      # Storage parameters that can be set: the union of the queue and archive default keys
      STORAGE_PARAMS = (
        Client::Autovacuum::DEFAULT_QUEUE_SETTINGS.keys + Client::Autovacuum::DEFAULT_ARCHIVE_SETTINGS.keys
      ).uniq.freeze

      self.error_messages = Contracts.error_messages("autovacuum")

      %i[queue_settings archive_settings].each do |key|
        required(key) { |settings| (settings.keys - STORAGE_PARAMS).empty? }
      end
    end
  end
end
