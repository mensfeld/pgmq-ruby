# frozen_string_literal: true

RSpec.describe PGMQ::Client::Maintenance, :integration do
  let(:client) { create_test_client }
  let(:queue_name) { test_queue_name }

  after do
    begin
      client.drop_queue(queue_name)
    rescue
      nil
    end
    client.close
  end

  before { ensure_test_queue(client, queue_name) }

  describe "#purge_queue" do
    it "purges all messages from queue" do
      batch = [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })]
      client.produce_batch(queue_name, batch)

      count = client.purge_queue(queue_name)
      expect(count).to eq("3")

      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end

    it "returns 0 for empty queue" do
      count = client.purge_queue(queue_name)
      expect(count).to eq("0")
    end
  end

  describe "#enable_notify_insert" do
    it "enables notifications on the queue" do
      expect { client.enable_notify_insert(queue_name) }.not_to raise_error
    end

    it "accepts custom throttle interval" do
      expect { client.enable_notify_insert(queue_name, throttle_interval_ms: 1000) }.not_to raise_error
    end

    it "accepts zero throttle interval for immediate notifications" do
      expect { client.enable_notify_insert(queue_name, throttle_interval_ms: 0) }.not_to raise_error
    end

    it "raises error for invalid queue name" do
      expect { client.enable_notify_insert("123invalid") }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
    end
  end

  describe "#disable_notify_insert" do
    it "disables notifications on the queue" do
      client.enable_notify_insert(queue_name)
      expect { client.disable_notify_insert(queue_name) }.not_to raise_error
    end

    it "raises error for invalid queue name" do
      expect { client.disable_notify_insert("123invalid") }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
    end
  end
end
