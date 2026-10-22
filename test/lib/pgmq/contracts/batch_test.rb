# frozen_string_literal: true

describe PGMQ::Contracts::Batch do
  let(:contract) { PGMQ::Contracts::Batch.new }

  it "accepts messages with nil headers or one header per message" do
    assert_predicate contract.call({ messages: [] }), :success?
    assert_predicate contract.call({ messages: ["{}"], headers: nil }), :success?
    assert_predicate contract.call({ messages: ["{}", "{}"], headers: ["{}", nil] }), :success?
  end

  it "rejects messages that are not an Array" do
    [nil, "{}", { "a" => 1 }].each do |messages|
      assert_equal({ messages: "must be an Array" }, contract.call({ messages: messages }).errors)
    end
  end

  it "rejects headers that are neither an Array nor nil" do
    ["{}", { "a" => 1 }].each do |headers|
      assert_equal(
        { headers: "must be an Array or nil" },
        contract.call({ messages: ["{}"], headers: headers }).errors
      )
    end
  end

  it "rejects headers with a different length than messages" do
    [[], ["{}", "{}"]].each do |headers|
      assert_equal(
        { headers: "must have one element per message" },
        contract.call({ messages: ["{}"], headers: headers }).errors
      )
    end
  end

  it "reports missing messages" do
    assert_equal({ messages: "needs to be present" }, contract.call({}).errors)
  end
end
