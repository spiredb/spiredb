defmodule Store.API.RESP.StreamCommandsTest do
  use ExUnit.Case, async: false

  alias Store.API.RESP.StreamCommands

  alias Store.Test.RocksDBHelper

  @test_stream "test_resp_stream_#{:erlang.unique_integer([:positive])}"

  setup do
    # Setup isolated RocksDB for this test file
    {:ok, _db, _cfs} =
      RocksDBHelper.setup_rocksdb("stream_commands_test_#{:erlang.unique_integer([:positive])}")

    on_exit(fn ->
      # Cleanup is handled by RocksDBHelper but we can ensure stream is gone
      :ok
    end)

    :ok
  end

  describe "XADD" do
    test "adds entry with auto-generated ID" do
      result = StreamCommands.execute(["XADD", @test_stream, "*", "field1", "value1"])

      assert is_binary(result)
      assert String.contains?(result, "-")
    end

    test "adds entry with explicit ID" do
      result =
        StreamCommands.execute(["XADD", @test_stream, "9999999999999-0", "field1", "value1"])

      assert result == "9999999999999-0"
    end

    test "adds entry with multiple fields" do
      result =
        StreamCommands.execute(["XADD", @test_stream, "*", "f1", "v1", "f2", "v2", "f3", "v3"])

      assert is_binary(result)
      assert String.contains?(result, "-")

      # Verify fields were stored
      {:ok, entries} = Store.Stream.xrange(@test_stream, "-", "+")
      assert length(entries) == 1
      {_id, fields} = hd(entries)
      assert length(fields) == 3
    end

    test "returns error for odd number of field-value pairs" do
      result = StreamCommands.execute(["XADD", @test_stream, "*", "field1"])

      assert match?({:error, _}, result)
    end

    test "supports MAXLEN option" do
      # Add 5 entries
      for i <- 1..5 do
        StreamCommands.execute(["XADD", @test_stream, "*", "n", "#{i}"])
      end

      # Add with MAXLEN
      StreamCommands.execute(["XADD", @test_stream, "MAXLEN", "3", "*", "n", "6"])

      {:ok, len} = Store.Stream.xlen(@test_stream)
      assert len == 3
    end

    test "supports MAXLEN ~ (approximate)" do
      result = StreamCommands.execute(["XADD", @test_stream, "MAXLEN", "~", "100", "*", "f", "v"])

      assert is_binary(result)
    end
  end

  describe "XLEN" do
    test "returns 0 for non-existent stream" do
      result = StreamCommands.execute(["XLEN", "nonexistent_stream_#{:rand.uniform(100_000)}"])

      assert result == 0
    end

    test "returns correct length" do
      StreamCommands.execute(["XADD", @test_stream, "*", "f", "v"])
      StreamCommands.execute(["XADD", @test_stream, "*", "f", "v"])

      result = StreamCommands.execute(["XLEN", @test_stream])

      assert result == 2
    end
  end

  describe "XRANGE" do
    setup do
      StreamCommands.execute(["XADD", @test_stream, "1000000000000-0", "n", "1"])
      StreamCommands.execute(["XADD", @test_stream, "1000000000001-0", "n", "2"])
      StreamCommands.execute(["XADD", @test_stream, "1000000000002-0", "n", "3"])
      :ok
    end

    test "returns all entries with - and +" do
      result = StreamCommands.execute(["XRANGE", @test_stream, "-", "+"])

      assert is_list(result)
      assert length(result) == 3
    end

    test "returns entries in format [[id, [field, value, ...]], ...]" do
      result = StreamCommands.execute(["XRANGE", @test_stream, "-", "+"])

      [[id, fields] | _] = result
      assert is_binary(id)
      assert is_list(fields)
    end

    test "respects COUNT option" do
      result = StreamCommands.execute(["XRANGE", @test_stream, "-", "+", "COUNT", "2"])

      assert length(result) == 2
    end

    test "respects start boundary" do
      result = StreamCommands.execute(["XRANGE", @test_stream, "1000000000001-0", "+"])

      assert length(result) == 2
    end
  end

  describe "XREVRANGE" do
    setup do
      StreamCommands.execute(["XADD", @test_stream, "1000000000000-0", "n", "1"])
      StreamCommands.execute(["XADD", @test_stream, "1000000000001-0", "n", "2"])
      StreamCommands.execute(["XADD", @test_stream, "1000000000002-0", "n", "3"])
      :ok
    end

    test "returns entries in reverse order" do
      result = StreamCommands.execute(["XREVRANGE", @test_stream, "+", "-"])

      assert length(result) == 3
      [[first_id, _] | _] = result
      assert first_id == "1000000000002-0"
    end
  end

  describe "XREAD" do
    setup do
      StreamCommands.execute(["XADD", @test_stream, "1000000000000-0", "n", "1"])
      StreamCommands.execute(["XADD", @test_stream, "1000000000001-0", "n", "2"])
      :ok
    end

    test "reads entries after ID" do
      result = StreamCommands.execute(["XREAD", "STREAMS", @test_stream, "1000000000000-0"])

      assert is_list(result)
      # Should have one stream with entries
      assert length(result) == 1
      [[_stream_name, entries]] = result
      assert length(entries) == 1
    end

    test "reads from beginning with 0-0" do
      result = StreamCommands.execute(["XREAD", "STREAMS", @test_stream, "0-0"])

      [[_stream_name, entries]] = result
      assert length(entries) == 2
    end

    test "supports COUNT option" do
      result = StreamCommands.execute(["XREAD", "COUNT", "1", "STREAMS", @test_stream, "0-0"])

      [[_stream_name, entries]] = result
      assert length(entries) == 1
    end
  end

  describe "XINFO" do
    test "returns stream info" do
      StreamCommands.execute(["XADD", @test_stream, "1000000000000-0", "n", "1"])
      StreamCommands.execute(["XADD", @test_stream, "1000000000001-0", "n", "2"])

      result = StreamCommands.execute(["XINFO", "STREAM", @test_stream])

      assert is_list(result)
      # Should contain length, first-entry, last-entry
      assert "length" in result
    end

    test "returns error for non-existent stream" do
      result =
        StreamCommands.execute(["XINFO", "STREAM", "nonexistent_#{:rand.uniform(100_000)}"])

      assert match?({:error, "ERR no such key"}, result)
    end
  end

  describe "XTRIM" do
    test "returns trimmed count for MAXLEN" do
      for i <- 1..10 do
        StreamCommands.execute(["XADD", @test_stream, "*", "n", "#{i}"])
      end

      result = StreamCommands.execute(["XTRIM", @test_stream, "MAXLEN", "5"])

      assert is_integer(result)
      assert result == 5
    end
  end
end
