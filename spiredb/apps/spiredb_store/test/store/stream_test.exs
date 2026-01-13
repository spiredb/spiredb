defmodule Store.StreamTest do
  use ExUnit.Case, async: false

  alias Store.Stream

  @test_stream "test_stream_#{:erlang.unique_integer([:positive])}"

  setup do
    # Setup RocksDB
    {:ok, _db, _cf_map} = Store.Test.RocksDBHelper.setup_rocksdb("stream_test")

    # Clean up test stream
    Stream.delete_stream(@test_stream)

    on_exit(fn ->
      Stream.delete_stream(@test_stream)
    end)

    :ok
  end

  describe "xadd/3" do
    test "adds entry and returns ID" do
      {:ok, id} = Stream.xadd(@test_stream, [{"field1", "value1"}])

      assert is_binary(id)
      assert String.contains?(id, "-")
    end

    test "auto-increments sequence for rapid adds" do
      {:ok, id1} = Stream.xadd(@test_stream, [{"n", "1"}])
      {:ok, id2} = Stream.xadd(@test_stream, [{"n", "2"}])
      {:ok, id3} = Stream.xadd(@test_stream, [{"n", "3"}])

      # IDs should be increasing
      assert id2 > id1
      assert id3 > id2
    end

    test "accepts explicit ID" do
      {:ok, id} = Stream.xadd(@test_stream, [{"test", "data"}], id: "9999999999999-42")

      assert id == "9999999999999-42"
    end

    test "rejects ID not greater than last" do
      {:ok, _} = Stream.xadd(@test_stream, [{"n", "1"}], id: "1000000000000-0")

      result = Stream.xadd(@test_stream, [{"n", "2"}], id: "1000000000000-0")
      assert result == {:error, :id_not_greater_than_last}
    end

    test "stores multiple fields" do
      fields = [{"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}]
      {:ok, _id} = Stream.xadd(@test_stream, fields)

      {:ok, entries} = Stream.xrange(@test_stream, "-", "+")
      assert length(entries) == 1

      {_id, stored_fields} = hd(entries)
      assert stored_fields == fields
    end
  end

  describe "xlen/1" do
    test "returns 0 for non-existent stream" do
      {:ok, len} = Stream.xlen("nonexistent_stream_#{:rand.uniform(100_000)}")
      assert len == 0
    end

    test "returns correct length after adds" do
      Stream.xadd(@test_stream, [{"n", "1"}])
      Stream.xadd(@test_stream, [{"n", "2"}])
      Stream.xadd(@test_stream, [{"n", "3"}])

      {:ok, len} = Stream.xlen(@test_stream)
      assert len == 3
    end
  end

  describe "xrange/4" do
    setup do
      # Add some entries with known IDs
      Stream.xadd(@test_stream, [{"n", "1"}], id: "1000000000000-0")
      Stream.xadd(@test_stream, [{"n", "2"}], id: "1000000000001-0")
      Stream.xadd(@test_stream, [{"n", "3"}], id: "1000000000002-0")
      Stream.xadd(@test_stream, [{"n", "4"}], id: "1000000000003-0")
      Stream.xadd(@test_stream, [{"n", "5"}], id: "1000000000004-0")

      :ok
    end

    test "returns all entries with - and +" do
      {:ok, entries} = Stream.xrange(@test_stream, "-", "+")

      assert length(entries) == 5
    end

    test "respects start boundary" do
      {:ok, entries} = Stream.xrange(@test_stream, "1000000000002-0", "+")

      assert length(entries) == 3
      {first_id, _} = hd(entries)
      assert first_id == "1000000000002-0"
    end

    test "respects end boundary" do
      {:ok, entries} = Stream.xrange(@test_stream, "-", "1000000000002-0")

      assert length(entries) == 3
      {last_id, _} = List.last(entries)
      assert last_id == "1000000000002-0"
    end

    test "respects count option" do
      {:ok, entries} = Stream.xrange(@test_stream, "-", "+", count: 2)

      assert length(entries) == 2
    end
  end

  describe "xread/2" do
    setup do
      Stream.xadd(@test_stream, [{"n", "1"}], id: "1000000000000-0")
      Stream.xadd(@test_stream, [{"n", "2"}], id: "1000000000001-0")
      Stream.xadd(@test_stream, [{"n", "3"}], id: "1000000000002-0")

      :ok
    end

    test "reads entries after given ID" do
      {:ok, results} = Stream.xread([{@test_stream, "1000000000000-0"}])

      [{stream_name, entries}] = results
      assert stream_name == @test_stream
      assert length(entries) == 2

      # Should not include 1000000000000-0
      ids = Enum.map(entries, fn {id, _fields} -> id end)
      refute "1000000000000-0" in ids
    end

    test "reads from beginning with 0-0" do
      {:ok, [{_name, entries}]} = Stream.xread([{@test_stream, "0-0"}])

      assert length(entries) == 3
    end

    test "returns empty for $ (latest)" do
      {:ok, [{_name, entries}]} = Stream.xread([{@test_stream, "$"}])

      # $ means "only new entries after now"
      assert entries == []
    end

    test "respects count option" do
      {:ok, [{_name, entries}]} = Stream.xread([{@test_stream, "0-0"}], count: 1)

      assert length(entries) == 1
    end
  end

  describe "xrevrange/4" do
    setup do
      Stream.xadd(@test_stream, [{"n", "1"}], id: "1000000000000-0")
      Stream.xadd(@test_stream, [{"n", "2"}], id: "1000000000001-0")
      Stream.xadd(@test_stream, [{"n", "3"}], id: "1000000000002-0")

      :ok
    end

    test "returns entries in reverse order" do
      {:ok, entries} = Stream.xrevrange(@test_stream, "+", "-")

      assert length(entries) == 3
      {first_id, _} = hd(entries)
      assert first_id == "1000000000002-0"
    end
  end

  describe "xinfo/1" do
    test "returns stream info" do
      Stream.xadd(@test_stream, [{"n", "1"}], id: "1000000000000-0")
      Stream.xadd(@test_stream, [{"n", "2"}], id: "1000000000001-0")

      {:ok, info} = Stream.xinfo(@test_stream)

      assert info.length == 2
      assert info.first_entry == "1000000000000-0"
      assert info.last_entry == "1000000000001-0"
    end

    test "returns error for non-existent stream" do
      result = Stream.xinfo("nonexistent_stream_#{:rand.uniform(100_000)}")
      assert result == {:error, :stream_not_found}
    end
  end

  describe "maxlen trimming" do
    test "trims stream to maxlen after xadd" do
      # Add 5 entries
      for i <- 1..5 do
        Stream.xadd(@test_stream, [{"n", "#{i}"}])
      end

      {:ok, len1} = Stream.xlen(@test_stream)
      assert len1 == 5

      # Add with maxlen: 3
      Stream.xadd(@test_stream, [{"n", "6"}], maxlen: 3)

      {:ok, len2} = Stream.xlen(@test_stream)
      assert len2 == 3
    end
  end

  describe "delete_stream/1" do
    test "deletes all entries and metadata" do
      Stream.xadd(@test_stream, [{"n", "1"}])
      Stream.xadd(@test_stream, [{"n", "2"}])

      {:ok, len1} = Stream.xlen(@test_stream)
      assert len1 == 2

      :ok = Stream.delete_stream(@test_stream)

      {:ok, len2} = Stream.xlen(@test_stream)
      assert len2 == 0
    end
  end
end
