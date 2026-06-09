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

  describe "#convert_archive_partitioned" do
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

    it "raises error for invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.convert_archive_partitioned("123invalid")
      end
    end

    it "returns nil when archive table does not exist (queue never created)" do
      result = @client.convert_archive_partitioned("nonexistent_queue_xyz")

      assert_nil result
    end

    it "converts archive table with default parameters and returns nil" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      result = @client.convert_archive_partitioned(@queue_name)

      assert_nil result
    end

    it "converts archive table with custom partition and retention intervals" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      result = @client.convert_archive_partitioned(@queue_name,
        partition_interval: "daily",
        retention_interval: "30 days",
        leading_partition: 5)

      assert_nil result
    end

    it "is idempotent when archive table is already partitioned" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      @client.convert_archive_partitioned(@queue_name)
      result = @client.convert_archive_partitioned(@queue_name)

      assert_nil result
    end
  end
end
