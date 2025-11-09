# frozen_string_literal: true

require_relative 'pgmq/version'
require_relative 'pgmq/errors'
require_relative 'pgmq/serializers/base'
require_relative 'pgmq/serializers/json'
require_relative 'pgmq/serializers/message_pack'
require_relative 'pgmq/message'
require_relative 'pgmq/metrics'
require_relative 'pgmq/queue_metadata'
require_relative 'pgmq/connection'
require_relative 'pgmq/transaction'
require_relative 'pgmq/client'

# PGMQ - Low-level Ruby client for Postgres Message Queue
#
# This is a low-level library providing direct access to PGMQ operations.
# For higher-level abstractions, job processing, and framework integrations,
# see pgmq-framework (similar to how rdkafka-ruby relates to Karafka).
#
# @example Basic usage
#   require 'pgmq'
#
#   # Create client with connection parameters
#   client = PGMQ::Client.new(
#     host: 'localhost',
#     port: 5432,
#     dbname: 'mydb',
#     user: 'postgres',
#     password: 'postgres'
#   )
#
#   # Or with connection string
#   client = PGMQ::Client.new('postgres://localhost/mydb')
#
#   # Basic queue operations
#   client.create('orders')
#   msg_id = client.send('orders', { order_id: 123 })
#   msg = client.read('orders', vt: 30)
#   client.delete('orders', msg.msg_id)
#   client.drop_queue('orders')
module PGMQ
  class << self
    # Convenience method to create a new client
    #
    # @param args [Array] arguments to pass to PGMQ::Client.new
    # @return [PGMQ::Client] new client instance
    #
    # @example
    #   client = PGMQ.new('postgres://localhost/mydb')
    def new(*, **)
      Client.new(*, **)
    end
  end
end
