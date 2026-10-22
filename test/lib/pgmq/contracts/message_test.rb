# frozen_string_literal: true

describe PGMQ::Contracts::Message do
  let(:contract) { PGMQ::Contracts::Message.new }

  it "accepts a String message with String or nil headers" do
    assert_predicate contract.call({ message: "{}", headers: nil }), :success?
    assert_predicate contract.call({ message: "{}", headers: "{}" }), :success?
    assert_predicate contract.call({ message: "{}" }), :success?
  end

  it "rejects a non-String message" do
    [nil, 1, 1.5, true, [1], { "a" => 1 }, :sym].each do |message|
      assert_equal({ message: "must be a JSON String" }, contract.call({ message: message }).errors)
    end
  end

  it "rejects headers that are neither a String nor nil" do
    [1, [1], { "a" => 1 }].each do |headers|
      assert_equal(
        { headers: "must be a JSON String or nil" },
        contract.call({ message: "{}", headers: headers }).errors
      )
    end
  end

  it "reports a missing message" do
    assert_equal({ message: "needs to be present" }, contract.call({}).errors)
  end
end
