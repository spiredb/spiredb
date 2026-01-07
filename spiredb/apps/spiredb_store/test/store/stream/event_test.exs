defmodule Store.Stream.EventTest do
  use ExUnit.Case, async: true

  alias Store.Stream.Event

  describe "generate_id/1" do
    test "generates ID with current timestamp" do
      {ts, seq} = Event.generate_id()
      now = System.system_time(:millisecond)

      assert ts >= now - 1000
      assert ts <= now + 1000
      assert seq == 0
    end

    test "increments sequence for same millisecond" do
      {ts1, seq1} = Event.generate_id()
      {ts2, seq2} = Event.generate_id(last_id: {ts1, seq1})

      if ts2 == ts1 do
        assert seq2 == seq1 + 1
      else
        assert seq2 == 0
      end
    end

    test "resets sequence for new millisecond" do
      old_ts = System.system_time(:millisecond) - 10_000
      {_ts, seq} = Event.generate_id(last_id: {old_ts, 999})

      assert seq == 0
    end

    test "uses explicit ID when provided" do
      result = Event.generate_id(explicit_id: "1234567890123-42")
      assert result == {1_234_567_890_123, 42}
    end
  end

  describe "parse_id/1 and format_id/1" do
    test "parses valid ID string" do
      assert Event.parse_id("1234567890123-42") == {1_234_567_890_123, 42}
    end

    test "returns :error for invalid ID" do
      assert Event.parse_id("invalid") == :error
      assert Event.parse_id("123-abc") == :error
      assert Event.parse_id("") == :error
    end

    test "formats ID tuple to string" do
      assert Event.format_id({1_234_567_890_123, 42}) == "1234567890123-42"
    end

    test "round-trips correctly" do
      id = {1_234_567_890_123, 42}
      assert Event.parse_id(Event.format_id(id)) == id
    end
  end

  describe "build_key/2 and parse_key/1" do
    test "builds padded key for proper ordering" do
      key = Event.build_key("mystream", {1_234_567_890_123, 42})

      # Should contain padded values
      assert String.contains?(key, "mystream:")
      assert String.contains?(key, "0001234567890123")
      assert String.contains?(key, "0000000042")
    end

    test "parses key back to components" do
      original_id = {1_234_567_890_123, 42}
      key = Event.build_key("mystream", original_id)

      assert {:ok, "mystream", ^original_id} = Event.parse_key(key)
    end

    test "handles stream names with colons" do
      id = {1_234_567_890_123, 0}
      key = Event.build_key("my:complex:stream", id)

      assert {:ok, "my:complex:stream", ^id} = Event.parse_key(key)
    end
  end

  describe "encode_fields/1 and decode_fields/1" do
    test "encodes and decodes simple fields" do
      fields = [{"field1", "value1"}, {"field2", "value2"}]
      encoded = Event.encode_fields(fields)

      assert {:ok, ^fields} = Event.decode_fields(encoded)
    end

    test "handles empty fields" do
      encoded = Event.encode_fields([])
      assert {:ok, []} = Event.decode_fields(encoded)
    end

    test "handles binary values" do
      fields = [{"binary", <<0, 1, 2, 3, 255>>}]
      encoded = Event.encode_fields(fields)

      assert {:ok, ^fields} = Event.decode_fields(encoded)
    end

    test "handles many fields" do
      fields = Enum.map(1..100, fn i -> {"field#{i}", "value#{i}"} end)
      encoded = Event.encode_fields(fields)

      assert {:ok, ^fields} = Event.decode_fields(encoded)
    end
  end

  describe "encode_entry/2 and decode_entry/1" do
    test "encodes and decodes complete entry" do
      id = {1_234_567_890_123, 42}
      fields = [{"name", "test"}, {"data", "value"}]

      encoded = Event.encode_entry(id, fields)

      assert {:ok, ^id, ^fields} = Event.decode_entry(encoded)
    end
  end

  describe "compare_ids/2" do
    test "compares by timestamp first" do
      assert Event.compare_ids({100, 0}, {200, 0}) == :lt
      assert Event.compare_ids({200, 0}, {100, 0}) == :gt
    end

    test "compares by sequence when timestamp equal" do
      assert Event.compare_ids({100, 1}, {100, 2}) == :lt
      assert Event.compare_ids({100, 2}, {100, 1}) == :gt
    end

    test "returns :eq for equal IDs" do
      assert Event.compare_ids({100, 5}, {100, 5}) == :eq
    end
  end

  describe "id_gt?/2 and id_gte?/2" do
    test "id_gt? returns correct results" do
      assert Event.id_gt?({100, 1}, {100, 0}) == true
      assert Event.id_gt?({100, 0}, {100, 0}) == false
      assert Event.id_gt?({100, 0}, {100, 1}) == false
    end

    test "id_gte? returns correct results" do
      assert Event.id_gte?({100, 1}, {100, 0}) == true
      assert Event.id_gte?({100, 0}, {100, 0}) == true
      assert Event.id_gte?({100, 0}, {100, 1}) == false
    end
  end

  describe "min_id/0 and max_id/0" do
    test "min_id is smallest possible" do
      assert Event.min_id() == {0, 0}
    end

    test "max_id is very large" do
      {ts, seq} = Event.max_id()
      assert ts > 9_000_000_000_000_000
      assert seq > 9_000_000_000
    end
  end
end
