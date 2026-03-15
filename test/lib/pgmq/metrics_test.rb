# frozen_string_literal: true

require "test_helper"

describe PGMQ::Metrics do
  before do
    @row = {
      "queue_name" => "orders",
      "queue_length" => "42",
      "newest_msg_age_sec" => "5",
      "oldest_msg_age_sec" => "120",
      "total_messages" => "1000",
      "scrape_time" => "2025-01-15 10:00:00 UTC"
    }
    @metrics = PGMQ::Metrics.new(@row)
  end

  describe "#initialize" do
    it "returns queue name as string" do
      assert_equal "orders", @metrics.queue_name
    end

    it "returns queue length as string" do
      assert_equal "42", @metrics.queue_length
    end

    it "returns newest message age as string" do
      assert_equal "5", @metrics.newest_msg_age_sec
    end

    it "returns oldest message age as string" do
      assert_equal "120", @metrics.oldest_msg_age_sec
    end

    it "returns total messages as string" do
      assert_equal "1000", @metrics.total_messages
    end

    it "returns scrape time as string" do
      assert_equal "2025-01-15 10:00:00 UTC", @metrics.scrape_time
    end
  end

  describe "#to_h" do
    it "returns hash representation" do
      hash = @metrics.to_h

      assert_equal "orders", hash[:queue_name]
      assert_equal "42", hash[:queue_length]
      assert_equal "1000", hash[:total_messages]
    end
  end

  describe "#inspect" do
    it "returns string representation" do
      assert_includes @metrics.inspect, "PGMQ::Metrics"
      # Data.define uses quotes for string values
      assert_includes @metrics.inspect, 'queue_name="orders"'
      assert_includes @metrics.inspect, 'queue_length="42"'
    end
  end

  context "with nil age values" do
    it "handles nil ages gracefully" do
      row_with_nils = @row.merge("newest_msg_age_sec" => nil, "oldest_msg_age_sec" => nil)
      m = PGMQ::Metrics.new(row_with_nils)

      assert_nil m.newest_msg_age_sec
      assert_nil m.oldest_msg_age_sec
    end
  end
end
