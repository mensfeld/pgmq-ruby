# frozen_string_literal: true

describe PGMQ::QueueName do
  describe ".valid?" do
    it "returns true for valid identifiers" do
      assert PGMQ::QueueName.valid?("orders")
      assert PGMQ::QueueName.valid?("My_Queue_1")
      assert PGMQ::QueueName.valid?("_private")
      assert PGMQ::QueueName.valid?("a" * 47)
    end

    it "returns false for invalid identifiers" do
      refute PGMQ::QueueName.valid?("my-queue")
      refute PGMQ::QueueName.valid?("123queue")
      refute PGMQ::QueueName.valid?("my.queue")
      refute PGMQ::QueueName.valid?("")
      refute PGMQ::QueueName.valid?("a" * 48)
      refute PGMQ::QueueName.valid?(nil)
    end
  end

  describe ".validate!" do
    it "returns the name unchanged when valid" do
      assert_equal "orders", PGMQ::QueueName.validate!("orders")
    end

    it "coerces non-string input to a String" do
      assert_equal "orders", PGMQ::QueueName.validate!(:orders)
    end

    it "raises for empty names" do
      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.validate!("") }
      assert_match(/cannot be empty/, e.message)
    end

    it "raises for nil" do
      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.validate!(nil) }
      assert_match(/cannot be empty/, e.message)
    end

    it "raises for whitespace-only names" do
      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.validate!("   ") }
      assert_match(/cannot be empty/, e.message)
    end

    it "raises with length detail for names that are too long" do
      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.validate!("a" * 48) }
      assert_match(/exceeds maximum length of 48 characters.*current length: 48/, e.message)
    end

    it "raises with identifier detail for illegal characters" do
      e = assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.validate!("my-queue") }
      assert_match(/must start with a letter or underscore/, e.message)
    end
  end

  describe ".normalize" do
    it "maps hyphens, dots, and colons to underscores" do
      assert_equal "chat_room_7", PGMQ::QueueName.normalize("chat:room-7")
      assert_equal "order_events", PGMQ::QueueName.normalize("order.events")
      assert_equal "Foo_Bar", PGMQ::QueueName.normalize("Foo-Bar")
    end

    it "strips other invalid characters rather than turning them into separators" do
      # An "@" is dropped, not mapped to "_", so "a@b" -> "ab" (not "a_b").
      assert_equal "ab", PGMQ::QueueName.normalize("a@b")
      assert_equal "orderevents", PGMQ::QueueName.normalize("order events")
    end

    it "maps colons to underscores so distinct turbo-rails streams do not collide" do
      # "a:b" must not collapse onto "ab" (the colon is the turbo-rails stream separator).
      refute_equal PGMQ::QueueName.normalize("ab"), PGMQ::QueueName.normalize("a:b")
      assert_equal "a_b", PGMQ::QueueName.normalize("a:b")
    end

    it "collapses repeated separators into a single underscore" do
      assert_equal "a_b_c", PGMQ::QueueName.normalize("a--b..c")
    end

    it "trims leading and trailing separators" do
      assert_equal "edge", PGMQ::QueueName.normalize("--edge--")
    end

    it "leaves an already-valid name unchanged" do
      assert_equal "orders", PGMQ::QueueName.normalize("orders")
    end

    it "raises when the normalized result starts with a digit" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.normalize("123-go") }
    end

    it "raises when nothing valid remains" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.normalize("!!!") }
    end

    it "raises when the normalized result exceeds the length limit" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.normalize("a-" * 30) }
    end
  end

  describe ".sanitize!" do
    it "strips invalid characters then returns the validated name" do
      assert_equal "orders", PGMQ::QueueName.sanitize!("orders!!")
      assert_equal "ab", PGMQ::QueueName.sanitize!("a-b")
      assert_equal "myqueue", PGMQ::QueueName.sanitize!("my.queue")
    end

    it "leaves an already-valid name unchanged" do
      assert_equal "My_Queue_1", PGMQ::QueueName.sanitize!("My_Queue_1")
    end

    it "raises rather than substituting when nothing valid remains" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.sanitize!("!!!") }
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.sanitize!("") }
    end

    it "raises when the stripped result would start with a digit" do
      # "123" survives stripping but is not a valid identifier — sanitize! must NOT silently mangle it.
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.sanitize!("123") }
    end

    it "raises when the stripped result is too long" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) { PGMQ::QueueName.sanitize!("a" * 80) }
    end
  end

  describe ".sanitize" do
    it "always returns a valid queue name" do
      [
        "99 Problems!",
        "Order-Events:Created",
        "MixedCASE",
        "x" * 80,
        "café au lait"
      ].each do |input|
        result = PGMQ::QueueName.sanitize(input)

        assert PGMQ::QueueName.valid?(result), "expected sanitize(#{input.inspect}) => #{result.inspect} to be valid"
      end
    end

    it "lowercases and underscores separators" do
      assert_equal "order_events_created", PGMQ::QueueName.sanitize("Order-Events:Created")
    end

    it "prefixes names that would start with a digit" do
      assert_equal "q_99_problems", PGMQ::QueueName.sanitize("99 Problems!")
      assert_equal "q_123", PGMQ::QueueName.sanitize("123")
    end

    it "falls back to a default when nothing usable remains" do
      assert_equal "queue", PGMQ::QueueName.sanitize("")
      assert_equal "queue", PGMQ::QueueName.sanitize("!!!")
      assert_equal "queue", PGMQ::QueueName.sanitize("___")
    end

    it "truncates to fit the length limit" do
      result = PGMQ::QueueName.sanitize("a" * 80)

      assert_operator result.length, :<, PGMQ::QueueName::MAX_LENGTH
      assert PGMQ::QueueName.valid?(result)
    end

    it "does not raise for arbitrary input" do
      assert_equal "queue", PGMQ::QueueName.sanitize(nil)
      PGMQ::QueueName.sanitize(12_345)
      PGMQ::QueueName.sanitize(:some_symbol)
    end

    it "produces a name a client can actually create" do
      client = create_test_client
      name = PGMQ::QueueName.sanitize("Tenant 42: Orders/Inbound!")

      begin
        assert client.create(name)
        assert_includes client.list_queues.map(&:queue_name), name
      ensure
        client.drop_queue(name)
        client.close
      end
    end
  end
end
