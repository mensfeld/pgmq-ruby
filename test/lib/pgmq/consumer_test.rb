# frozen_string_literal: true

describe PGMQ::Consumer do
  before { setup_client_and_queue("consumer") }
  after { teardown_client_and_queue }

  def build_consumer(**options)
    PGMQ::Consumer.new(@client, @queue_name, max_poll_seconds: 1, poll_interval_ms: 10, **options)
  end

  describe "#initialize" do
    it "exposes the client and queue names" do
      consumer = build_consumer

      assert_same @client, consumer.client
      assert_equal [@queue_name], consumer.queue_names
      refute_predicate consumer, :stopped?
    end

    it "accepts several queue names" do
      consumer = PGMQ::Consumer.new(@client, [@queue_name, "other_queue"])

      assert_equal [@queue_name, "other_queue"], consumer.queue_names
    end

    it "rejects an empty, too long or duplicated list of queue names" do
      assert_raises(PGMQ::Errors::ConfigurationError) { PGMQ::Consumer.new(@client, []) }
      assert_raises(PGMQ::Errors::ConfigurationError) do
        PGMQ::Consumer.new(@client, Array.new(51) { |i| "queue_#{i}" })
      end
      assert_raises(PGMQ::Errors::ConfigurationError) { PGMQ::Consumer.new(@client, [@queue_name, @queue_name]) }
      assert_raises(PGMQ::Errors::ConfigurationError) do
        PGMQ::Consumer.new(@client, [@queue_name, @queue_name.upcase])
      end
      assert_raises(PGMQ::Errors::ConfigurationError) do
        PGMQ::Consumer.new(@client, [@queue_name.to_sym, @queue_name])
      end
    end

    it "rejects an invalid name in a list of queue names" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        PGMQ::Consumer.new(@client, [@queue_name, "1invalid"])
      end
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::Consumer.new(@client, [@queue_name, nil]) }
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::Consumer.new(@client, [1]) }
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
      @client.stubs(:set_vt_batch).raises(PGMQ::Errors::ConnectionError, "down")

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

    it "sets queue_name on messages from a single queue" do
      @client.produce(@queue_name, to_json_msg({ n: 1 }))
      consumer = build_consumer
      names = []

      consumer.each do |message|
        names << message.queue_name
        consumer.stop
      end

      assert_equal [@queue_name], names
    end

    it "propagates client errors" do
      consumer = PGMQ::Consumer.new(@client, unique_queue_name("missing"), max_poll_seconds: 1)

      assert_raises(PGMQ::Errors::ConnectionError) { consumer.each { flunk "should not yield" } }
    end
  end

  describe "with several queues" do
    before do
      @other_queue_name = unique_queue_name("consumer_other")
      ensure_test_queue(@client, @other_queue_name)
    end

    after do
      @client.drop_queue(@other_queue_name)
    rescue
      nil
    end

    def build_multi_consumer(**options)
      PGMQ::Consumer.new(
        @client, [@queue_name, @other_queue_name], max_poll_seconds: 1, poll_interval_ms: 10, **options
      )
    end

    it "yields messages from every queue and acknowledges each on its own queue" do
      2.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }
      2.times { |i| @client.produce(@other_queue_name, to_json_msg({ n: i })) }
      consumer = build_multi_consumer(on_success: :archive)
      received = []

      consumer.each do |message|
        received << [message.queue_name, JSON.parse(message.message)["n"]]
        consumer.stop if received.size == 4
      end

      expected = [[@queue_name, 0], [@queue_name, 1], [@other_queue_name, 0], [@other_queue_name, 1]]

      assert_equal expected.sort, received.sort
      assert_empty @client.read_batch(@queue_name, vt: 0, qty: 10)
      assert_empty @client.read_batch(@other_queue_name, vt: 0, qty: 10)

      [@queue_name, @other_queue_name].each do |queue_name|
        archived = @client.connection.with_connection { |conn| conn.exec("SELECT msg_id FROM pgmq.a_#{queue_name}").to_a }

        assert_equal 2, archived.size
      end
    end

    it "deletes messages on their own queue" do
      2.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }
      2.times { |i| @client.produce(@other_queue_name, to_json_msg({ n: i })) }
      consumer = build_multi_consumer
      received = 0

      consumer.each do
        received += 1
        consumer.stop if received == 4
      end

      [@queue_name, @other_queue_name].each do |queue_name|
        count = @client.connection.with_connection do |conn|
          conn.exec("SELECT count(*) FROM pgmq.q_#{queue_name}").getvalue(0, 0)
        end

        assert_equal "0", count
      end
    end

    it "archives messages that exceeded max_attempts on their own queue" do
      msg_id = @client.produce(@other_queue_name, to_json_msg({ n: 1 }))
      2.times { @client.read(@other_queue_name, vt: 0) }
      consumer = build_multi_consumer(max_attempts: 2)
      yielded = []
      archived = {}

      thread = Thread.new { consumer.each { |message| yielded << message } }
      wait_for(timeout: 5) do
        archived = [@queue_name, @other_queue_name].to_h do |queue_name|
          rows = @client.connection.with_connection do |conn|
            conn.exec("SELECT msg_id FROM pgmq.a_#{queue_name}").map { |row| row["msg_id"] }
          end

          [queue_name, rows]
        end
        archived[@other_queue_name].any?
      end
      consumer.stop
      thread.join

      assert_empty yielded
      assert_empty archived[@queue_name]
      assert_equal [msg_id], archived[@other_queue_name]
    end

    it "does not starve a queue listed after a busy one" do
      5.times { |i| @client.produce(@queue_name, to_json_msg({ n: i })) }
      @client.produce(@other_queue_name, to_json_msg({ n: 0 }))
      consumer = build_multi_consumer(max_attempts: 3)
      received = []

      consumer.each do |message|
        received << message.queue_name
        consumer.stop if received.size == 2
      end

      assert_includes received, @other_queue_name
    end

    it "reads one message per poll with max_attempts, so a failure leaves other queues untouched" do
      @client.produce(@queue_name, to_json_msg({ n: 1 }))
      @client.produce(@other_queue_name, to_json_msg({ n: 2 }))
      consumer = build_multi_consumer(max_attempts: 1)

      assert_raises(RuntimeError) { consumer.each { raise "boom" } }

      read_counts = [@queue_name, @other_queue_name].map do |queue_name|
        @client.connection.with_connection do |conn|
          conn.exec("SELECT read_ct FROM pgmq.q_#{queue_name}").map { |row| row["read_ct"] }
        end
      end

      assert_equal [["1"], ["0"]], read_counts
    end

    it "releases not yet yielded messages on their own queues" do
      @client.produce(@queue_name, to_json_msg({ n: 1 }))
      @client.produce(@other_queue_name, to_json_msg({ n: 2 }))
      consumer = build_multi_consumer(vt: 30)
      yielded = []

      assert_raises(RuntimeError) do
        consumer.each do |message|
          yielded << message.queue_name
          raise "boom"
        end
      end

      released = [@queue_name, @other_queue_name] - yielded

      assert_equal 1, @client.read_batch(released.first, vt: 0, qty: 10).size
      assert_empty @client.read_batch(yielded.first, vt: 0, qty: 10)
    end
  end
end
