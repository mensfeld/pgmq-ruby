# frozen_string_literal: true

describe PGMQ::Contracts::MultiQueue do
  let(:contract) { PGMQ::Contracts::MultiQueue.new }

  it "accepts valid arguments" do
    assert_predicate contract.call({ queue_names: ["a"] }), :success?
    assert_predicate contract.call({ queue_names: Array.new(50) { |i| "q#{i}" } }), :success?
    assert_predicate contract.call({ deletions: {} }), :success?
    assert_predicate contract.call({ archives: { "a" => [1] } }), :success?
    assert_predicate contract.call({ updates: { "a" => [1] } }), :success?
  end

  it "rejects queue_names that are not an Array" do
    assert_equal({ queue_names: "must be an Array" }, contract.call({ queue_names: "a" }).errors)
  end

  it "rejects empty queue_names" do
    assert_equal({ queue_names: "cannot be empty" }, contract.call({ queue_names: [] }).errors)
  end

  it "rejects more than 50 queue_names" do
    assert_equal(
      { queue_names: "cannot exceed 50 queues" },
      contract.call({ queue_names: Array.new(51) { |i| "q#{i}" } }).errors
    )
  end

  %i[deletions archives updates].each do |key|
    it "rejects #{key} that is not a Hash" do
      assert_equal({ key => "must be a Hash" }, contract.call({ key => [] }).errors)
    end
  end
end
