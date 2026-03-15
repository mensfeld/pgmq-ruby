# frozen_string_literal: true

describe PGMQ::Client do
  before { @client = PGMQ::Client.new(TEST_DB_PARAMS) }

  describe "initialization" do
    it "accepts a PGMQ::Connection object" do
      connection = PGMQ::Connection.new(TEST_DB_PARAMS)
      injected_client = PGMQ::Client.new(connection)

      assert_equal connection, injected_client.connection

      # Verify it works
      queue_name = unique_queue_name("inject")
      injected_client.create(queue_name)
      queues = injected_client.list_queues

      assert_includes queues.map(&:queue_name), queue_name

      injected_client.drop_queue(queue_name)
      injected_client.close
    end

    it "creates connection from connection string" do
      client = PGMQ::Client.new("postgres://postgres:postgres@localhost:5433/pgmq_test")

      assert_kind_of PGMQ::Connection, client.connection
      client.close
    end

    it "creates connection from hash parameters" do
      client = PGMQ::Client.new(TEST_DB_PARAMS)

      assert_kind_of PGMQ::Connection, client.connection
      client.close
    end
  end

  describe "#validate_queue_name!" do
    describe "valid queue names" do
      it "accepts simple lowercase names" do
        @client.__send__(:validate_queue_name!, "my_queue")
      end

      it "accepts names starting with uppercase letter" do
        @client.__send__(:validate_queue_name!, "MyQueue")
      end

      it "accepts names starting with underscore" do
        @client.__send__(:validate_queue_name!, "_private_queue")
      end

      it "accepts names with numbers" do
        @client.__send__(:validate_queue_name!, "queue123")
      end

      it "accepts mixed case with underscores and numbers" do
        @client.__send__(:validate_queue_name!, "My_Queue_123")
      end

      it "accepts names up to 47 characters" do
        long_name = "a" * 47
        @client.__send__(:validate_queue_name!, long_name)
      end
    end

    describe "invalid queue names" do
      it "rejects nil" do
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, nil)
        end
        assert_match(/cannot be empty/, e.message)
      end

      it "rejects empty string" do
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, "")
        end
        assert_match(/cannot be empty/, e.message)
      end

      it "rejects whitespace-only string" do
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, "   ")
        end
        assert_match(/cannot be empty/, e.message)
      end

      it "rejects names starting with number" do
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, "123queue")
        end
        assert_match(/must start with a letter or underscore/, e.message)
      end

      it "rejects names with hyphens" do
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, "my-queue")
        end
        assert_match(/must start with a letter or underscore/, e.message)
      end

      it "rejects names with spaces" do
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, "my queue")
        end
        assert_match(/must start with a letter or underscore/, e.message)
      end

      it "rejects names with special characters" do
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, "my.queue")
        end
        assert_match(/must start with a letter or underscore/, e.message)
      end

      it "rejects names with 48 characters" do
        long_name = "a" * 48
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, long_name)
        end
        assert_match(/exceeds maximum length of 48 characters.*current length: 48/, e.message)
      end

      it "rejects names with 60 characters" do
        long_name = "a" * 60
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, long_name)
        end
        assert_match(/exceeds maximum length of 48 characters.*current length: 60/, e.message)
      end

      it "includes queue name in length error message" do
        long_name = "my_very_long_queue_name_that_exceeds_the_limit_48chars"
        e = assert_raises(PGMQ::Errors::InvalidQueueNameError) do
          @client.__send__(:validate_queue_name!, long_name)
        end
        assert_match(/Queue name '#{Regexp.escape(long_name)}'/, e.message)
      end
    end
  end
end
