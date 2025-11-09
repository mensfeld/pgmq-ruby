# frozen_string_literal: true

RSpec.describe PGMQ::Serializers::Base do
  subject(:serializer) { described_class.new }

  describe '#serialize' do
    it 'raises NotImplementedError when not overridden' do
      expect { serializer.serialize({ foo: 'bar' }) }.to raise_error(
        NotImplementedError,
        /must implement #serialize/
      )
    end
  end

  describe '#deserialize' do
    it 'raises NotImplementedError when not overridden' do
      expect { serializer.deserialize('{"foo":"bar"}') }.to raise_error(
        NotImplementedError,
        /must implement #deserialize/
      )
    end
  end
end
