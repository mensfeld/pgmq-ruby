# frozen_string_literal: true

describe PGMQ::Client::Maintenance do
  before { setup_client_and_queue }
  after { teardown_client_and_queue }

  describe "#purge_queue" do
    it "purges all messages from queue" do
      batch = [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })]
      @client.produce_batch(@queue_name, batch)

      count = @client.purge_queue(@queue_name)

      assert_equal "3", count

      msg = @client.read(@queue_name, vt: 30)

      assert_nil msg
    end

    it "returns 0 for empty queue" do
      count = @client.purge_queue(@queue_name)

      assert_equal "0", count
    end
  end

  describe "#enable_notify_insert" do
    it "enables notifications on the queue" do
      @client.enable_notify_insert(@queue_name)
    end

    it "accepts custom throttle interval" do
      @client.enable_notify_insert(@queue_name, throttle_interval_ms: 1000)
    end

    it "accepts zero throttle interval for immediate notifications" do
      @client.enable_notify_insert(@queue_name, throttle_interval_ms: 0)
    end

    it "raises error for invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.enable_notify_insert("123invalid")
      end
    end
  end

  describe "#disable_notify_insert" do
    it "disables notifications on the queue" do
      @client.enable_notify_insert(@queue_name)
      @client.disable_notify_insert(@queue_name)
    end

    it "raises error for invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.disable_notify_insert("123invalid")
      end
    end
  end
end
