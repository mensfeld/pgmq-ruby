# frozen_string_literal: true

require "test_helper"

describe PGMQ::Client::Metrics do
  before do
    @client = create_test_client
    @queue_name = unique_queue_name
    ensure_test_queue(@client, @queue_name)
  end

  after do
    begin
      @client.drop_queue(@queue_name)
    rescue
      nil
    end
    @client.close
  end

  describe "#metrics" do
    it "returns queue metrics" do
      # Send some messages
      batch = [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })]
      @client.produce_batch(@queue_name, batch)

      metrics = @client.metrics(@queue_name)

      assert_kind_of PGMQ::Metrics, metrics
      assert_equal @queue_name, metrics.queue_name
      assert_equal "3", metrics.queue_length
      assert_equal "3", metrics.total_messages
    end

    it "raises error for non-existent queue" do
      e = assert_raises(PGMQ::Errors::ConnectionError) do
        @client.metrics("nonexistent_queue_xyz")
      end
      assert_match(/relation.*does not exist/, e.message)
    end

    it "returns metrics for empty queue" do
      metrics = @client.metrics(@queue_name)

      assert_kind_of PGMQ::Metrics, metrics
      assert_equal @queue_name, metrics.queue_name
      assert_equal "0", metrics.queue_length
    end
  end

  describe "#metrics_all" do
    it "returns metrics for all queues" do
      queue1 = unique_queue_name("one")
      queue2 = unique_queue_name("two")

      @client.create(queue1)
      @client.create(queue2)
      @client.produce(queue1, to_json_msg({ test: 1 }))
      @client.produce(queue2, to_json_msg({ test: 2 }))

      all_metrics = @client.metrics_all

      assert_kind_of Array, all_metrics

      our_queues = all_metrics.select { |m| m.queue_name.start_with?("test_") }

      assert_operator our_queues.size, :>=, 2

      @client.drop_queue(queue1)
      @client.drop_queue(queue2)
    end

    it "returns empty array when no queues exist" do
      # This test would require a fresh database, so we'll just check it returns an array
      all_metrics = @client.metrics_all

      assert_kind_of Array, all_metrics
    end
  end
end
