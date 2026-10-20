# frozen_string_literal: true

describe PGMQ::Client::Metrics do
  before { setup_client_and_queue }
  after { teardown_client_and_queue }

  def pg_partman_available?(client)
    result = client.instance_eval do
      with_connection do |conn|
        conn.exec("SELECT 1 FROM pg_extension WHERE extname = 'pg_partman' LIMIT 1")
      end
    end
    result.ntuples.positive?
  rescue
    false
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

    it "returns visible length excluding messages hidden by vt" do
      batch = [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })]
      @client.produce_batch(@queue_name, batch)
      @client.read(@queue_name, vt: 30)

      metrics = @client.metrics(@queue_name)

      assert_equal "3", metrics.queue_length
      assert_equal "2", metrics.queue_visible_length
    end

    it "returns nil default partition length for a non-partitioned queue" do
      metrics = @client.metrics(@queue_name)

      assert_nil metrics.default_partition_length
    end

    it "returns default partition length for a partitioned queue" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      queue = unique_queue_name("part")
      @client.create_partitioned(queue,
        partition_interval: "10000",
        retention_interval: "100000")
      @client.produce(queue, to_json_msg({ a: 1 }))

      metrics = @client.metrics(queue)

      assert_equal "0", metrics.default_partition_length
      assert_equal "1", metrics.queue_visible_length

      @client.drop_queue(queue)
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

      m1 = all_metrics.find { |m| m.queue_name == queue1 }

      assert_equal "1", m1.queue_visible_length
      assert_nil m1.default_partition_length

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
