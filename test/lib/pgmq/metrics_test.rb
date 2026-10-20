# frozen_string_literal: true

describe PGMQ::Metrics do
  before do
    @row = {
      "queue_name" => "orders",
      "queue_length" => "42",
      "newest_msg_age_sec" => "5",
      "oldest_msg_age_sec" => "120",
      "total_messages" => "1000",
      "scrape_time" => "2025-01-15 10:00:00 UTC",
      "queue_visible_length" => "40",
      "default_partition_length" => "0"
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

    it "returns queue visible length as string" do
      assert_equal "40", @metrics.queue_visible_length
    end

    it "returns default partition length as string" do
      assert_equal "0", @metrics.default_partition_length
    end
  end

  describe "#to_h" do
    it "returns hash representation" do
      hash = @metrics.to_h

      assert_equal "orders", hash[:queue_name]
      assert_equal "42", hash[:queue_length]
      assert_equal "1000", hash[:total_messages]
      assert_equal "40", hash[:queue_visible_length]
      assert_equal "0", hash[:default_partition_length]
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

  context "with a row from an older PGMQ version" do
    it "returns nil for columns the row does not have" do
      old_row = @row.except("queue_visible_length", "default_partition_length")
      m = PGMQ::Metrics.new(old_row)

      assert_nil m.queue_visible_length
      assert_nil m.default_partition_length
    end
  end
end
