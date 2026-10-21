# frozen_string_literal: true

describe PGMQ::Contractable::Contract do
  let(:error_messages) do
    {
      "missing" => "needs to be present",
      "id_format" => "needs to be a String",
      "test_format" => "needs to be 5",
      "name_format" => "needs to be name",
      "id2_format" => "needs to be a String"
    }
  end

  let(:contract_class) do
    messages = error_messages

    Class.new(PGMQ::Contractable::Contract) do
      self.error_messages = messages

      required(:id) { |id| id.is_a?(String) }

      optional(:test) { |test| test == 5 }
      optional(:name) { |name| name == "name" }
    end
  end

  let(:contract) { contract_class.new }

  describe "#validate!" do
    it "returns true for valid data" do
      assert contract.validate!({ id: "1" }, ArgumentError)
      assert contract.validate!({ id: "1", name: "name" }, ArgumentError)
    end

    it "raises for an invalid required value" do
      e = assert_raises(ArgumentError) { contract.validate!({ id: 1 }, ArgumentError) }

      assert_match(/needs to be a String/, e.message)
    end

    it "raises for an invalid optional value" do
      assert_raises(ArgumentError) { contract.validate!({ id: "1", test: Time.now }, ArgumentError) }
      assert_raises(ArgumentError) { contract.validate!({ id: "1", name: Time.now }, ArgumentError) }
      assert_raises(ArgumentError) { contract.validate!({ id: "1", test: nil }, ArgumentError) }
    end

    it "raises for a missing required value" do
      e = assert_raises(ArgumentError) { contract.validate!({}, ArgumentError) }

      assert_match(/needs to be present/, e.message)
    end

    it "raises the given error class" do
      assert_raises(PGMQ::Errors::ConfigurationError) do
        contract.validate!({ id: 1 }, PGMQ::Errors::ConfigurationError)
      end
    end

    it "validates with a scope" do
      assert contract.validate!({ id: "1" }, ArgumentError, scope: %w[a b])
      assert_raises(ArgumentError) { contract.validate!({ id: 1 }, ArgumentError, scope: %w[a b]) }
    end

    it "raises KeyError when no error message matches" do
      klass = Class.new(PGMQ::Contractable::Contract) do
        self.error_messages = {}

        required(:na_id) { |id| id.is_a?(String) }
      end

      assert_raises(KeyError) { klass.new.validate!({ na_id: 1 }, ArgumentError) }
    end
  end

  describe "#call" do
    it "returns a cached success result for valid data" do
      result = contract.call({ id: "1" })

      assert_predicate result, :success?
      assert_empty result.errors
      assert_same PGMQ::Contractable::Result.success, result
    end

    it "nests the error path in the scope" do
      result = contract.call({ id: 1 }, scope: ["messages", 3])

      assert_equal({ "messages.3.id": "needs to be a String" }, result.errors)
    end

    it "uses a String returned by a rule as the error message" do
      klass = Class.new(PGMQ::Contractable::Contract) do
        required(:id) { |_id| "custom error" }
      end

      assert_equal({ id: "custom error" }, klass.new.call({ id: 1 }).errors)
    end

    it "reports required paths as missing when the root is not a Hash" do
      klass = Class.new(PGMQ::Contractable::Contract) do
        self.error_messages = { "missing" => "needs to be present" }

        required(:id) { |id| id.is_a?(String) }
        required(:a, :b) { |v| v.is_a?(String) }
      end

      result = klass.new.call("not-a-hash")

      refute_predicate result, :success?
      assert_equal %i[id a.b], result.errors.keys
    end
  end

  context "with nested values" do
    let(:contract_class) do
      messages = error_messages

      Class.new(PGMQ::Contractable::Contract) do
        self.error_messages = messages

        nested(:nested) do
          required(:id) { |id| id.is_a?(String) }
          optional(:id2) { |id| id.is_a?(String) }
        end
      end
    end

    it "validates nested data" do
      assert contract.validate!({ nested: { id: "1" } }, ArgumentError)
      assert contract.validate!({ nested: { id: "1", id2: "2" } }, ArgumentError)
    end

    it "reports nested errors with dot notation" do
      assert_equal({ "nested.id2": "needs to be a String" }, contract.call({ nested: { id: "1", id2: 2 } }).errors)
      assert_equal({ "nested.id": "needs to be present" }, contract.call({ id: 1 }).errors)
    end

    it "reports a missing path when an intermediate value is not a Hash" do
      assert_equal({ "nested.id": "needs to be present" }, contract.call({ nested: "x" }).errors)
    end
  end

  context "with multiple nestings" do
    let(:contract_class) do
      messages = error_messages

      Class.new(PGMQ::Contractable::Contract) do
        self.error_messages = messages

        nested(:a) do
          nested(:b) do
            nested(:c) do
              required(:id) { |id| id.is_a?(String) }
            end
          end
        end
      end
    end

    it "validates deeply nested data" do
      assert contract.validate!({ a: { b: { c: { id: "1" } } } }, ArgumentError)
      assert_equal({ "a.b.c.id": "needs to be a String" }, contract.call({ a: { b: { c: { id: 1 } } } }).errors)
    end

    it "reports a missing path when an intermediate value is not a Hash" do
      assert_equal({ "a.b.c.id": "needs to be present" }, contract.call({ a: { b: "x" } }).errors)
      assert_equal({ "a.b.c.id": "needs to be present" }, contract.call({ a: { b: {} } }).errors)
    end
  end

  context "when a nested block raises during definition" do
    it "does not leak the nested path onto later rules" do
      klass = Class.new(PGMQ::Contractable::Contract) do
        begin
          nested(:a) { raise "boom" }
        rescue
          nil
        end

        required(:id) { |id| id.is_a?(String) }
      end

      assert_equal %i[id], klass.rules.last.path
    end
  end

  context "with virtual rules" do
    it "reports the errors a virtual rule returns, within the scope" do
      klass = Class.new(PGMQ::Contractable::Contract) do
        virtual { |_data, _errors, _contract| [[%i[id], "boom"]] }
      end

      contract = klass.new

      assert_equal({ "outer.id": "boom" }, contract.call({}, scope: %i[outer]).errors)
      assert_equal({ "outer.id": "boom" }, contract.call({}, scope: %i[outer]).errors)
      assert_raises(ArgumentError) { contract.validate!({}, ArgumentError) }
    end

    it "treats a non-Array result as no errors" do
      [false, nil, true].each do |returned|
        klass = Class.new(PGMQ::Contractable::Contract) do
          virtual { |_data, _errors, _contract| returned }
        end

        assert_predicate klass.new.call({}), :success?
      end
    end
  end
end
