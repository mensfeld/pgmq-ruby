# frozen_string_literal: true

RSpec.describe "PGMQ::Errors" do
  describe "error hierarchy" do
    it "has BaseError inheriting from StandardError" do
      expect(PGMQ::Errors::BaseError.superclass).to eq(StandardError)
    end

    it "has ConnectionError inheriting from BaseError" do
      expect(PGMQ::Errors::ConnectionError.superclass).to eq(PGMQ::Errors::BaseError)
    end

    it "has QueueNotFoundError inheriting from BaseError" do
      expect(PGMQ::Errors::QueueNotFoundError.superclass).to eq(PGMQ::Errors::BaseError)
    end

    it "has MessageNotFoundError inheriting from BaseError" do
      expect(PGMQ::Errors::MessageNotFoundError.superclass).to eq(PGMQ::Errors::BaseError)
    end

    it "has SerializationError inheriting from BaseError" do
      expect(PGMQ::Errors::SerializationError.superclass).to eq(PGMQ::Errors::BaseError)
    end

    it "has ConfigurationError inheriting from BaseError" do
      expect(PGMQ::Errors::ConfigurationError.superclass).to eq(PGMQ::Errors::BaseError)
    end

    it "has InvalidQueueNameError inheriting from BaseError" do
      expect(PGMQ::Errors::InvalidQueueNameError.superclass).to eq(PGMQ::Errors::BaseError)
    end
  end

  describe "error instantiation" do
    it "creates ConnectionError with message" do
      error = PGMQ::Errors::ConnectionError.new("test connection error")
      expect(error.message).to eq("test connection error")
    end

    it "creates InvalidQueueNameError with message" do
      error = PGMQ::Errors::InvalidQueueNameError.new("invalid name")
      expect(error.message).to eq("invalid name")
    end
  end
end
