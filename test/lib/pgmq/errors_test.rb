# frozen_string_literal: true

describe "PGMQ::Errors" do
  describe "error hierarchy" do
    it "has BaseError inheriting from StandardError" do
      assert_equal StandardError, PGMQ::Errors::BaseError.superclass
    end

    it "has ConnectionError inheriting from BaseError" do
      assert_equal PGMQ::Errors::BaseError, PGMQ::Errors::ConnectionError.superclass
    end

    it "has QueueNotFoundError inheriting from BaseError" do
      assert_equal PGMQ::Errors::BaseError, PGMQ::Errors::QueueNotFoundError.superclass
    end

    it "has MessageNotFoundError inheriting from BaseError" do
      assert_equal PGMQ::Errors::BaseError, PGMQ::Errors::MessageNotFoundError.superclass
    end

    it "has SerializationError inheriting from BaseError" do
      assert_equal PGMQ::Errors::BaseError, PGMQ::Errors::SerializationError.superclass
    end

    it "has ConfigurationError inheriting from BaseError" do
      assert_equal PGMQ::Errors::BaseError, PGMQ::Errors::ConfigurationError.superclass
    end

    it "has InvalidQueueNameError inheriting from BaseError" do
      assert_equal PGMQ::Errors::BaseError, PGMQ::Errors::InvalidQueueNameError.superclass
    end
  end

  describe "error instantiation" do
    it "creates ConnectionError with message" do
      error = PGMQ::Errors::ConnectionError.new("test connection error")

      assert_equal "test connection error", error.message
    end

    it "creates InvalidQueueNameError with message" do
      error = PGMQ::Errors::InvalidQueueNameError.new("invalid name")

      assert_equal "invalid name", error.message
    end
  end
end
