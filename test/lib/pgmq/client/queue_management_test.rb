# frozen_string_literal: true

describe PGMQ::Client::QueueManagement do
  before do
    @client = create_test_client
    @queue_name = unique_queue_name
  end

  after { teardown_client_and_queue }

  describe "#create" do
    it "creates a new queue and returns true" do
      result = @client.create(@queue_name)

      assert result
      queues = @client.list_queues

      assert_includes queues.map(&:queue_name), @queue_name
    end

    it "returns false when queue already exists" do
      @client.create(@queue_name)
      result = @client.create(@queue_name)

      refute result
    end

    it "raises error for invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create("123invalid")
      end
    end

    it "raises error for empty queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create("")
      end
    end

    it "creates queue with maximum length name (47 chars)" do
      long_name = "a" * 47

      @client.create(long_name)

      queues = @client.list_queues

      assert_includes queues.map(&:queue_name), long_name

      @client.drop_queue(long_name)
    end

    it "rejects queue name at boundary (48 chars)" do
      too_long_name = "a" * 48

      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create(too_long_name)
      end
      assert_match(/exceeds maximum length of 48 characters.*current length: 48/, e.message)
    end

    it "rejects queue name exceeding limit (60 chars)" do
      way_too_long = "q" * 60

      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create(way_too_long)
      end
      assert_match(/exceeds maximum length of 48 characters.*current length: 60/, e.message)
    end

    it "provides clear error messages for invalid names" do
      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create("123invalid")
      end
      assert_match(/must start with a letter or underscore/, e.message)

      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create("my-queue")
      end
      assert_match(/must start with a letter or underscore/, e.message)

      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create("")
      end
      assert_match(/cannot be empty/, e.message)
    end
  end

  describe "#create_partitioned" do
    after { @client.drop_queue(@default_queue) if @default_queue }

    it "creates a partitioned queue and returns true" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      result = @client.create_partitioned(@queue_name,
        partition_interval: "10000",
        retention_interval: "100000")

      assert result
      queues = @client.list_queues

      assert_includes queues.map(&:queue_name), @queue_name
    end

    it "returns false when queue already exists" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      @client.create_partitioned(@queue_name,
        partition_interval: "10000",
        retention_interval: "100000")
      result = @client.create_partitioned(@queue_name,
        partition_interval: "10000",
        retention_interval: "100000")

      refute result
    end

    def partition_count(client, table)
      result = client.instance_eval do
        with_connection do |conn|
          conn.exec_params("SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass", [table])
        end
      end
      result[0]["count"].to_i
    end

    def stub_create_connection(client)
      conn = mock("conn")
      conn.stubs(:exec_params).returns(stub(ntuples: 0))
      client.stubs(:with_connection).yields(conn)
      conn
    end

    it "pre-makes the requested number of partitions for the queue and archive" do
      skip "pg_partman not installed" unless pg_partman_available?(@client)

      default_queue = @default_queue = unique_queue_name
      @client.create_partitioned(default_queue,
        partition_interval: "10000",
        retention_interval: "100000")
      @client.create_partitioned(@queue_name,
        partition_interval: "10000",
        retention_interval: "100000",
        premake: 8)

      default_count = partition_count(@client, "pgmq.q_#{default_queue}")

      assert_equal default_count + 4, partition_count(@client, "pgmq.q_#{@queue_name}")
      assert_equal partition_count(@client, "pgmq.a_#{default_queue}") + 4,
        partition_count(@client, "pgmq.a_#{@queue_name}")
    end

    it "keeps the 3-argument SQL call when premake is not given" do
      conn = stub_create_connection(@client)
      conn.expects(:exec_params)
        .with("SELECT pgmq.create_partitioned($1::text, $2::text, $3::text)", [@queue_name, "10000", "100000"])
        .once

      @client.create_partitioned(@queue_name,
        partition_interval: "10000",
        retention_interval: "100000")
    end

    it "passes premake as the 4th SQL argument" do
      conn = stub_create_connection(@client)
      conn.expects(:exec_params)
        .with(
          "SELECT pgmq.create_partitioned($1::text, $2::text, $3::text, $4::integer)",
          [@queue_name, "10000", "100000", 12]
        )
        .once

      @client.create_partitioned(@queue_name,
        partition_interval: "10000",
        retention_interval: "100000",
        premake: 12)
    end

    it "raises error for invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create_partitioned("123invalid",
          partition_interval: "10000",
          retention_interval: "100000")
      end
    end
  end

  describe "#create_unlogged" do
    it "creates an unlogged queue and returns true" do
      result = @client.create_unlogged(@queue_name)

      assert result
      queues = @client.list_queues

      assert_includes queues.map(&:queue_name), @queue_name

      # Verify it works by sending/reading a message
      msg_id = @client.produce(@queue_name, to_json_msg({ test: "unlogged" }))
      msg = @client.read(@queue_name, vt: 30)

      assert_equal msg_id, msg.msg_id
      assert_equal "unlogged", JSON.parse(msg.message)["test"]
    end

    it "returns false when queue already exists" do
      @client.create_unlogged(@queue_name)
      result = @client.create_unlogged(@queue_name)

      refute result
    end

    it "raises error for invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create_unlogged("123invalid")
      end
    end
  end

  describe "#drop_queue" do
    it "drops a queue" do
      @client.create(@queue_name)
      result = @client.drop_queue(@queue_name)

      assert result

      queues = @client.list_queues

      refute_includes queues.map(&:queue_name), @queue_name
    end

    it "returns false for non-existent queue" do
      result = @client.drop_queue("nonexistent_queue_xyz")

      refute result
    end
  end

  describe "#list_queues" do
    it "lists all queues" do
      queue1 = unique_queue_name("list1")
      queue2 = unique_queue_name("list2")

      @client.create(queue1)
      @client.create(queue2)

      queues = @client.list_queues
      queue_names = queues.map(&:queue_name)

      assert_includes queue_names, queue1
      assert_includes queue_names, queue2
      assert_kind_of PGMQ::QueueMetadata, queues.first

      @client.drop_queue(queue1)
      @client.drop_queue(queue2)
    end
  end

  describe "#create_fifo_index" do
    it "creates the FIFO index on a queue and returns nil" do
      @client.create(@queue_name)

      result = @client.create_fifo_index(@queue_name)

      assert_nil result
    end

    it "is idempotent - calling twice does not raise" do
      @client.create(@queue_name)
      @client.create_fifo_index(@queue_name)

      result = @client.create_fifo_index(@queue_name)

      assert_nil result
    end

    it "raises for invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.create_fifo_index("123invalid")
      end
    end
  end

  describe "#create_fifo_indexes_all" do
    it "creates FIFO indexes for all queues and returns nil" do
      queue1 = unique_queue_name("fifo1")
      queue2 = unique_queue_name("fifo2")

      @client.create(queue1)
      @client.create(queue2)

      result = @client.create_fifo_indexes_all

      assert_nil result

      @client.drop_queue(queue1)
      @client.drop_queue(queue2)
    end

    it "is idempotent - calling twice does not raise" do
      @client.create(@queue_name)
      @client.create_fifo_indexes_all

      result = @client.create_fifo_indexes_all

      assert_nil result
    end
  end
end
