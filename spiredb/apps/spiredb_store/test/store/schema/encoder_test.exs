defmodule Store.Schema.EncoderTest do
  @moduledoc """
  Tests for binary-safe key encoding.
  """

  use ExUnit.Case, async: true

  alias Store.Schema.Encoder

  describe "table key encoding" do
    test "encodes table key with integer table_id" do
      key = Encoder.encode_table_key(1, "user:123")

      # 4 bytes for table_id + pk bytes
      assert byte_size(key) == 4 + byte_size("user:123")
      assert <<1::32, "user:123">> = key
    end

    test "decodes table key" do
      key = Encoder.encode_table_key(42, "my_pk")
      {table_id, pk} = Encoder.decode_table_key(key)

      assert table_id == 42
      assert pk == "my_pk"
    end

    test "table_id is big-endian for sortability" do
      key1 = Encoder.encode_table_key(1, "pk")
      key2 = Encoder.encode_table_key(256, "pk")

      # key1 should sort before key2
      assert key1 < key2
    end
  end

  describe "index key encoding" do
    test "encodes index key with separator" do
      key = Encoder.encode_index_key(1, "email@test.com", "user:123")

      # Should contain null separator
      assert :binary.match(key, <<0x00>>) != :nomatch
    end

    test "decodes index key" do
      key = Encoder.encode_index_key(5, "indexed_value", "pk123")
      {index_id, indexed, pk} = Encoder.decode_index_key(key)

      assert index_id == 5
      # indexed_value is sortable-encoded
      assert is_binary(indexed)
      assert pk == "pk123"
    end

    test "index prefix for range scan" do
      prefix = Encoder.encode_index_prefix(42)

      assert prefix == <<42::32>>

      # All keys for index 42 should start with this prefix
      key = Encoder.encode_index_key(42, "val", "pk")
      assert :binary.match(key, prefix) == {0, 4}
    end

    test "index value prefix for exact match" do
      prefix = Encoder.encode_index_value_prefix(10, "email@test.com")

      # Should end with separator
      assert :binary.last(prefix) == 0x00
    end
  end

  describe "vector key encoding" do
    test "encodes vector key" do
      key = Encoder.encode_vector_key(5, "doc:123")

      {index_id, doc_id} = Encoder.decode_vector_key(key)
      assert index_id == 5
      assert doc_id == "doc:123"
    end
  end

  describe "vector value encoding with payload" do
    test "encodes vector without payload" do
      vector_bytes = <<1.0::float-32-native, 2.0::float-32-native>>
      encoded = Encoder.encode_vector_value(vector_bytes, nil)

      # Should have vector + 4 bytes (payload_len = 0)
      assert byte_size(encoded) == byte_size(vector_bytes) + 4
    end

    test "encodes vector with binary payload" do
      vector_bytes = <<1.0::float-32-native>>
      payload = ~s({"title":"test"})

      encoded = Encoder.encode_vector_value(vector_bytes, payload)

      # Should be larger than just vector + 4
      assert byte_size(encoded) > byte_size(vector_bytes) + 4
    end

    test "encodes vector with map payload" do
      vector_bytes = <<1.0::float-32-native>>
      payload = %{"title" => "test", "score" => 0.95}

      encoded = Encoder.encode_vector_value(vector_bytes, payload)
      assert is_binary(encoded)
    end
  end

  describe "sortable value encoding" do
    test "positive integers sort correctly" do
      encoded1 = Encoder.encode_sortable_value(1)
      encoded2 = Encoder.encode_sortable_value(100)
      encoded3 = Encoder.encode_sortable_value(1000)

      assert encoded1 < encoded2
      assert encoded2 < encoded3
    end

    test "negative integers sort correctly" do
      encoded_neg = Encoder.encode_sortable_value(-100)
      encoded_zero = Encoder.encode_sortable_value(0)
      encoded_pos = Encoder.encode_sortable_value(100)

      assert encoded_neg < encoded_zero
      assert encoded_zero < encoded_pos
    end

    test "floats sort correctly" do
      encoded1 = Encoder.encode_sortable_value(1.5)
      encoded2 = Encoder.encode_sortable_value(2.5)

      assert encoded1 < encoded2
    end

    test "strings preserve order" do
      encoded_a = Encoder.encode_sortable_value("apple")
      encoded_b = Encoder.encode_sortable_value("banana")

      assert encoded_a < encoded_b
    end

    test "booleans sort correctly" do
      encoded_false = Encoder.encode_sortable_value(false)
      encoded_true = Encoder.encode_sortable_value(true)

      assert encoded_false < encoded_true
    end

    test "nil encodes to smallest value" do
      encoded_nil = Encoder.encode_sortable_value(nil)
      encoded_other = Encoder.encode_sortable_value(0)

      assert encoded_nil < encoded_other
    end
  end

  describe "transaction key encoding" do
    test "txn_data_key has inverted timestamp for newest-first" do
      key1 = Encoder.encode_txn_data_key("mykey", 1000)
      key2 = Encoder.encode_txn_data_key("mykey", 2000)

      # key2 (newer) should sort before key1 (older)
      assert key2 < key1
    end

    test "decodes txn_data_key" do
      key = Encoder.encode_txn_data_key("test_key", 12345)
      {decoded_key, decoded_ts} = Encoder.decode_txn_data_key(key)

      assert decoded_key == "test_key"
      assert decoded_ts == 12345
    end

    test "txn_write_key has inverted timestamp" do
      key1 = Encoder.encode_txn_write_key("mykey", 1000)
      key2 = Encoder.encode_txn_write_key("mykey", 2000)

      # key2 (newer commit) should sort before key1
      assert key2 < key1
    end

    test "decodes txn_write_key" do
      key = Encoder.encode_txn_write_key("commit_key", 99999)
      {decoded_key, decoded_ts} = Encoder.decode_txn_write_key(key)

      assert decoded_key == "commit_key"
      assert decoded_ts == 99999
    end
  end
end
