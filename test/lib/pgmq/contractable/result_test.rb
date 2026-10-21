# frozen_string_literal: true

describe PGMQ::Contractable::Result do
  let(:contract_class) do
    Class.new(PGMQ::Contractable::Contract) do
      self.error_messages = {
        "format" => "is invalid",
        "id_format" => "needs to be a String",
        "test_format" => "needs to be 5"
      }

      required(:id) { |id| id.is_a?(String) }
      optional(:test) { |test| test == 5 }
      optional(:other) { |other| other == 1 }
    end
  end

  let(:contract) { contract_class.new }

  it "has no errors on success" do
    result = contract.call({ id: "123", test: 5 })

    assert_predicate result, :success?
    assert_empty result.errors
  end

  it "maps error keys to messages" do
    result = contract.call({ id: 123, test: 4 })

    refute_predicate result, :success?
    assert_equal({ id: "needs to be a String", test: "needs to be 5" }, result.errors)
  end

  it "falls back to the error key message when the path has none" do
    assert_equal({ other: "is invalid" }, contract.call({ id: "1", other: 2 }).errors)
  end

  it "falls back from a dynamic scope to the key's own message" do
    assert_equal({ "messages.7.id": "needs to be a String" }, contract.call({ id: 1 }, scope: ["messages", 7]).errors)
  end
end
