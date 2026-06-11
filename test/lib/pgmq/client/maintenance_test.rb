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

  describe "#wait_for_notify" do
    it "returns nil when timeout expires with no notification" do
      result = @client.wait_for_notify(@queue_name, timeout: 0.1)

      assert_nil result
    end

    it "receives a notification sent from another thread" do
      queue_name = @queue_name
      channel = "pgmq.q_#{queue_name}.INSERT"

      Thread.new do
        sleep 0.1
        @client.instance_eval { with_connection { |c| c.exec("NOTIFY \"#{channel}\"") } }
      end

      result = @client.wait_for_notify(queue_name, timeout: 3)

      refute_nil result
    end

    it "yields channel, pid, and payload to the block" do
      queue_name = @queue_name
      channel = "pgmq.q_#{queue_name}.INSERT"
      yielded_channel = nil
      yielded_pid = nil

      Thread.new do
        sleep 0.1
        @client.instance_eval { with_connection { |c| c.exec("NOTIFY \"#{channel}\", 'hello'") } }
      end

      @client.wait_for_notify(queue_name, timeout: 3) do |ch, pid, _payload|
        yielded_channel = ch
        yielded_pid = pid
      end

      assert_equal channel, yielded_channel
      assert_kind_of Integer, yielded_pid
    end

    it "unlistens even when an exception is raised inside the block" do
      queue_name = @queue_name
      channel = "pgmq.q_#{queue_name}.INSERT"

      Thread.new do
        sleep 0.1
        @client.instance_eval { with_connection { |c| c.exec("NOTIFY \"#{channel}\"") } }
      end

      assert_raises(RuntimeError) do
        @client.wait_for_notify(queue_name, timeout: 3) { raise "boom" }
      end

      # Connection should be returned to pool and UNLISTEN should have fired;
      # a subsequent call must not hang or error
      result = @client.wait_for_notify(queue_name, timeout: 0.1)
      assert_nil result
    end

    it "works end-to-end with enable_notify_insert" do
      @client.enable_notify_insert(@queue_name)
      queue_name = @queue_name

      Thread.new do
        sleep 0.2
        @client.produce(queue_name, '{"event":"test"}')
      end

      result = @client.wait_for_notify(@queue_name, timeout: 3)

      refute_nil result
    ensure
      @client.disable_notify_insert(@queue_name)
    end

    it "raises error for invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.wait_for_notify("123invalid")
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

    it "returns nil when archive table does not exist (early return before pg_partman is touched)" do
      # pgmq checks table existence first; a missing archive table returns without error regardless
      # of whether pg_partman is installed - this verifies that early-return path
      result = @client.convert_archive_partitioned("nonexistent_queue_xyz")

      assert_nil result
    end

    it "raises ConnectionError when archive table exists but pg_partman is not installed" do
      skip "pg_partman is installed" if pg_partman_available?(@client)

      msg_id = @client.produce(@queue_name, to_json_msg({ x: 1 }))
      @client.archive(@queue_name, msg_id)

      assert_raises(PGMQ::Errors::ConnectionError) do
        @client.convert_archive_partitioned(@queue_name)
      end
    end

    it "converts archive table and it becomes partitioned" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      msg_id = @client.produce(@queue_name, to_json_msg({ x: 1 }))
      @client.archive(@queue_name, msg_id)

      @client.convert_archive_partitioned(@queue_name)

      queue = @queue_name
      is_partitioned = @client.instance_eval do
        with_connection do |conn|
          result = conn.exec_params(
            "SELECT relkind FROM pg_class " \
            "JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace " \
            "WHERE nspname = 'pgmq' AND relname = $1",
            ["a_#{queue}"]
          )
          result.ntuples.positive? && result[0]["relkind"] == "p"
        end
      end

      assert is_partitioned, "archive table should be partitioned (relkind='p') after conversion"
    end

    it "converts archive table with custom partition and retention intervals" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      @client.convert_archive_partitioned(@queue_name,
        partition_interval: "daily",
        retention_interval: "30 days",
        leading_partition: 5)

      queue = @queue_name
      is_partitioned = @client.instance_eval do
        with_connection do |conn|
          result = conn.exec_params(
            "SELECT relkind FROM pg_class " \
            "JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace " \
            "WHERE nspname = 'pgmq' AND relname = $1",
            ["a_#{queue}"]
          )
          result.ntuples.positive? && result[0]["relkind"] == "p"
        end
      end

      assert is_partitioned
    end

    it "is idempotent when archive table is already partitioned" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      @client.convert_archive_partitioned(@queue_name)

      result = @client.convert_archive_partitioned(@queue_name)

      assert_nil result
    end
  end
end
