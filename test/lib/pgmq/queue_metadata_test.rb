# frozen_string_literal: true

require "test_helper"

describe PGMQ::QueueMetadata do
  before do
    @row = {
      "queue_name" => "my_queue",
      "created_at" => "2025-01-15 09:00:00 UTC",
      "is_partitioned" => "t",
      "is_unlogged" => "f"
    }
    @metadata = PGMQ::QueueMetadata.new(@row)
  end

  describe "#initialize" do
    it "returns queue name as string" do
      assert_equal "my_queue", @metadata.queue_name
    end

    it "returns created_at as string timestamp" do
      assert_equal "2025-01-15 09:00:00 UTC", @metadata.created_at
    end

    it "returns is_partitioned as PostgreSQL boolean string" do
      assert_equal "t", @metadata.is_partitioned
    end

    it "returns is_unlogged as PostgreSQL boolean string" do
      assert_equal "f", @metadata.is_unlogged
    end
  end

  describe "#partitioned?" do
    it "returns partitioned status" do
      assert_equal "t", @metadata.partitioned?
    end
  end

  describe "#unlogged?" do
    it "returns unlogged status" do
      assert_equal "f", @metadata.unlogged?
    end
  end

  describe "#to_h" do
    it "returns hash representation" do
      hash = @metadata.to_h

      assert_equal "my_queue", hash[:queue_name]
      assert_equal "t", hash[:is_partitioned]
      assert_equal "f", hash[:is_unlogged]
    end
  end

  describe "#inspect" do
    it "returns string representation" do
      assert_includes @metadata.inspect, "PGMQ::QueueMetadata"
      # Data.define uses quotes for string values
      assert_includes @metadata.inspect, 'queue_name="my_queue"'
      assert_includes @metadata.inspect, 'is_partitioned="t"'
    end
  end

  context "with different PostgreSQL boolean representations" do
    it 'handles PostgreSQL boolean "f"' do
      row_with_f = @row.merge("is_partitioned" => "f", "is_unlogged" => "f")
      m = PGMQ::QueueMetadata.new(row_with_f)

      assert_equal "f", m.is_partitioned
      assert_equal "f", m.is_unlogged
    end

    it 'handles PostgreSQL boolean "t"' do
      row_with_t = @row.merge("is_partitioned" => "t", "is_unlogged" => "t")
      m = PGMQ::QueueMetadata.new(row_with_t)

      assert_equal "t", m.is_partitioned
      assert_equal "t", m.is_unlogged
    end
  end
end
