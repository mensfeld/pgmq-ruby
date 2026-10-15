# frozen_string_literal: true

describe PGMQ::Consumer do
  before { setup_client_and_queue("consumer") }
  after { teardown_client_and_queue }

  def build_consumer(**options)
    PGMQ::Consumer.new(@client, @queue_name, max_poll_seconds: 1, poll_interval_ms: 10, **options)
  end

  describe "#initialize" do
    it "exposes the client and queue name" do
      consumer = build_consumer

      assert_same @client, consumer.client
      assert_equal @queue_name, consumer.queue_name
      refute_predicate consumer, :stopped?
    end

    it "rejects an invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        PGMQ::Consumer.new(@client, "1invalid")
      end
    end

    it "rejects an unknown on_success action" do
      error = assert_raises(PGMQ::Errors::ConfigurationError) do
        build_consumer(on_success: :purge)
      end

      assert_match(/on_success/, error.message)
    end

    %i[vt batch_size max_poll_seconds poll_interval_ms max_attempts].each do |option|
      it "rejects a non-positive #{option}" do
        assert_raises(PGMQ::Errors::ConfigurationError) { build_consumer(option => 0) }
        assert_raises(PGMQ::Errors::ConfigurationError) { build_consumer(option => "1") }
      end
    end

    it "rejects max_attempts with a batch_size above 1" do
      error = assert_raises(PGMQ::Errors::ConfigurationError) do
        build_consumer(batch_size: 3, max_attempts: 1)
      end

      assert_match(/batch_size: 1/, error.message)
    end
  end

  describe "#each" do
    it "requires a block" do
      assert_raises(ArgumentError) { build_consumer.each }
    end

    it "yields messages in order and deletes them after processing" do
      3.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }
      consumer = build_consumer(batch_size: 2)
      received = []

      result = consumer.each do |message|
        received << JSON.parse(message.message)["n"]
        consumer.stop if received.size == 3
      end

      assert_nil result
      assert_equal [0, 1, 2], received
      assert_empty @client.read_batch(@queue_name, vt: 0, qty: 10)
      assert_empty @client.connection.with_connection { |c| c.exec("SELECT * FROM pgmq.a_#{@queue_name}").to_a }
    end

    it "archives messages when on_success is :archive" do
      msg_id = @client.produce(@queue_name, to_json_msg({ n: 1 }))
      consumer = build_consumer(on_success: :archive)

      consumer.each { consumer.stop }

      archived = @client.connection.with_connection do |conn|
        conn.exec("SELECT msg_id FROM pgmq.a_#{@queue_name}").map { |row| row["msg_id"] }
      end

      assert_equal [msg_id], archived
      assert_empty @client.read_batch(@queue_name, vt: 0, qty: 10)
    end

    it "re-raises a block error without acknowledging the message" do
      msg_id = @client.produce(@queue_name, to_json_msg({ n: 1 }))
      consumer = build_consumer(vt: 1)

      assert_raises(RuntimeError) { consumer.each { raise "boom" } }

      redelivered = nil
      wait_for(timeout: 5) { redelivered = @client.read(@queue_name, vt: 30) }

      assert_equal msg_id, redelivered.msg_id
      assert_equal "2", redelivered.read_ct
    end

    it "releases the not yet yielded messages of the batch when the block raises" do
      3.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }
      consumer = build_consumer(batch_size: 3, vt: 30)

      assert_raises(RuntimeError) { consumer.each { raise "boom" } }

      remaining = @client.read_batch(@queue_name, vt: 30, qty: 10)

      assert_equal([1, 2], remaining.map { |m| JSON.parse(m.message)["n"] })
    end

    it "keeps the block error when releasing the batch fails" do
      3.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }
      consumer = build_consumer(batch_size: 3, vt: 30)
      @client.define_singleton_method(:set_vt_batch) { |*, **| raise PGMQ::Errors::ConnectionError, "down" }

      error = assert_raises(RuntimeError) { consumer.each { raise "boom" } }

      assert_equal "boom", error.message
    end

    it "processes the rest of the batch when stopped mid-batch" do
      3.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }
      consumer = build_consumer(batch_size: 3, vt: 30)
      received = []

      consumer.each do |message|
        received << JSON.parse(message.message)["n"]
        consumer.stop
      end

      assert_equal [0, 1, 2], received
      assert_empty @client.read_batch(@queue_name, vt: 0, qty: 10)
    end

    it "does not count a stop as an attempt for a restarted consumer with max_attempts" do
      3.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }
      received = []

      first = build_consumer(max_attempts: 1)
      first.each do |message|
        received << JSON.parse(message.message)["n"]
        first.stop
      end

      second = build_consumer(max_attempts: 1)
      second.each do |message|
        received << JSON.parse(message.message)["n"]
        second.stop if received.size == 3
      end

      assert_equal [0, 1, 2], received
      assert_empty @client.connection.with_connection { |c| c.exec("SELECT * FROM pgmq.a_#{@queue_name}").to_a }
    end

    it "does not count a block error against other messages for a restarted consumer with max_attempts" do
      3.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }

      assert_raises(RuntimeError) { build_consumer(max_attempts: 1, vt: 30).each { raise "boom" } }

      received = []
      second = build_consumer(max_attempts: 1, vt: 30)
      second.each do |message|
        received << JSON.parse(message.message)["n"]
        second.stop if received.size == 2
      end

      assert_equal [1, 2], received
      assert_empty @client.connection.with_connection { |c| c.exec("SELECT * FROM pgmq.a_#{@queue_name}").to_a }
    end

    it "archives messages that exceeded max_attempts without yielding them" do
      msg_id = @client.produce(@queue_name, to_json_msg({ n: 1 }))
      2.times { @client.read(@queue_name, vt: 0) }
      consumer = build_consumer(max_attempts: 2)
      yielded = []

      thread = Thread.new { consumer.each { |message| yielded << message } }
      archived = []
      wait_for(timeout: 5) do
        archived = @client.connection.with_connection do |conn|
          conn.exec("SELECT msg_id, read_ct FROM pgmq.a_#{@queue_name}").to_a
        end
        archived.any?
      end
      consumer.stop
      thread.join

      assert_empty yielded
      assert_equal [msg_id], archived.map { |row| row["msg_id"] }
      assert_equal "3", archived.first["read_ct"]
    end

    it "yields messages that are within max_attempts" do
      @client.produce(@queue_name, to_json_msg({ n: 1 }))
      @client.read(@queue_name, vt: 0)
      consumer = build_consumer(max_attempts: 2)
      yielded = []

      consumer.each do |message|
        yielded << message
        consumer.stop
      end

      assert_equal ["2"], yielded.map(&:read_ct)
    end

    it "returns at once when already stopped" do
      @client.produce(@queue_name, to_json_msg({ n: 1 }))
      consumer = build_consumer
      consumer.stop

      consumer.each { flunk "should not yield" }

      assert_equal 1, @client.read_batch(@queue_name, vt: 0, qty: 10).size
    end

    it "returns after the current poll when stopped on an empty queue" do
      consumer = build_consumer
      thread = Thread.new { consumer.each { flunk "should not yield" } }

      sleep 0.2
      consumer.stop

      assert thread.join(5)
      assert_predicate consumer, :stopped?
    end

    it "propagates client errors" do
      consumer = PGMQ::Consumer.new(@client, unique_queue_name("missing"), max_poll_seconds: 1)

      assert_raises(PGMQ::Errors::ConnectionError) { consumer.each { flunk "should not yield" } }
    end
  end
end
