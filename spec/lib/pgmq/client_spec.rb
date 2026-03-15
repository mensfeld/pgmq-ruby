# frozen_string_literal: true

RSpec.describe PGMQ::Client do
  let(:client) { described_class.new(TEST_DB_PARAMS) }

  describe "initialization", :integration do
    it "accepts a PGMQ::Connection object" do
      connection = PGMQ::Connection.new(TEST_DB_PARAMS)
      injected_client = described_class.new(connection)

      expect(injected_client.connection).to eq(connection)

      # Verify it works
      queue_name = test_queue_name("inject")
      injected_client.create(queue_name)
      queues = injected_client.list_queues
      expect(queues.map(&:queue_name)).to include(queue_name)

      injected_client.drop_queue(queue_name)
      injected_client.close
    end

    it "creates connection from connection string" do
      client = described_class.new("postgres://postgres:postgres@localhost:5433/pgmq_test")
      expect(client.connection).to be_a(PGMQ::Connection)
      client.close
    end

    it "creates connection from hash parameters" do
      client = described_class.new(TEST_DB_PARAMS)
      expect(client.connection).to be_a(PGMQ::Connection)
      client.close
    end
  end

  describe "#validate_queue_name!" do
    describe "valid queue names" do
      it "accepts simple lowercase names" do
        expect { client.__send__(:validate_queue_name!, "my_queue") }.not_to raise_error
      end

      it "accepts names starting with uppercase letter" do
        expect { client.__send__(:validate_queue_name!, "MyQueue") }.not_to raise_error
      end

      it "accepts names starting with underscore" do
        expect { client.__send__(:validate_queue_name!, "_private_queue") }.not_to raise_error
      end

      it "accepts names with numbers" do
        expect { client.__send__(:validate_queue_name!, "queue123") }.not_to raise_error
      end

      it "accepts mixed case with underscores and numbers" do
        expect { client.__send__(:validate_queue_name!, "My_Queue_123") }.not_to raise_error
      end

      it "accepts names up to 47 characters" do
        long_name = "a" * 47
        expect { client.__send__(:validate_queue_name!, long_name) }.not_to raise_error
      end
    end

    describe "invalid queue names" do
      it "rejects nil" do
        expect { client.__send__(:validate_queue_name!, nil) }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /cannot be empty/
        )
      end

      it "rejects empty string" do
        expect { client.__send__(:validate_queue_name!, "") }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /cannot be empty/
        )
      end

      it "rejects whitespace-only string" do
        expect { client.__send__(:validate_queue_name!, "   ") }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /cannot be empty/
        )
      end

      it "rejects names starting with number" do
        expect { client.__send__(:validate_queue_name!, "123queue") }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /must start with a letter or underscore/
        )
      end

      it "rejects names with hyphens" do
        expect { client.__send__(:validate_queue_name!, "my-queue") }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /must start with a letter or underscore/
        )
      end

      it "rejects names with spaces" do
        expect { client.__send__(:validate_queue_name!, "my queue") }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /must start with a letter or underscore/
        )
      end

      it "rejects names with special characters" do
        expect { client.__send__(:validate_queue_name!, "my.queue") }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /must start with a letter or underscore/
        )
      end

      it "rejects names with 48 characters" do
        long_name = "a" * 48
        expect { client.__send__(:validate_queue_name!, long_name) }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /exceeds maximum length of 48 characters.*current length: 48/
        )
      end

      it "rejects names with 60 characters" do
        long_name = "a" * 60
        expect { client.__send__(:validate_queue_name!, long_name) }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /exceeds maximum length of 48 characters.*current length: 60/
        )
      end

      it "includes queue name in length error message" do
        long_name = "my_very_long_queue_name_that_exceeds_the_limit_48chars"
        expect { client.__send__(:validate_queue_name!, long_name) }.to raise_error(
          PGMQ::Errors::InvalidQueueNameError,
          /Queue name '#{Regexp.escape(long_name)}'/
        )
      end
    end
  end
end
