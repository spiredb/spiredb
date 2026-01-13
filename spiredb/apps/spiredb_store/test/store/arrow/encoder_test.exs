defmodule Store.Arrow.EncoderTest do
  use ExUnit.Case, async: true

  alias Store.Arrow.Encoder
  alias Explorer.DataFrame, as: DF

  describe "encode_scan_batch/1" do
    test "encodes empty batch" do
      rows = []
      binary = Encoder.encode_scan_batch(rows)

      assert is_binary(binary)

      # Verify Arrow IPC format by reading back
      {:ok, df} = DF.load_ipc_stream(binary)
      assert DF.n_rows(df) == 0
      assert DF.names(df) == ["key", "value"]
    end

    test "encodes single row" do
      rows = [{"key1", "value1"}]
      binary = Encoder.encode_scan_batch(rows)

      assert is_binary(binary)

      {:ok, df} = DF.load_ipc_stream(binary)
      assert DF.n_rows(df) == 1
      assert DF.names(df) == ["key", "value"]

      # Verify data
      data = DF.to_rows(df)
      assert data == [%{"key" => "key1", "value" => "value1"}]
    end

    test "encodes multiple rows" do
      rows = [
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
      ]

      binary = Encoder.encode_scan_batch(rows)

      assert is_binary(binary)

      {:ok, df} = DF.load_ipc_stream(binary)
      assert DF.n_rows(df) == 3

      data = DF.to_rows(df)

      assert data == [
               %{"key" => "key1", "value" => "value1"},
               %{"key" => "key2", "value" => "value2"},
               %{"key" => "key3", "value" => "value3"}
             ]
    end

    test "handles binary keys and values" do
      rows = [
        {<<1, 2, 3>>, <<4, 5, 6>>},
        {<<7, 8, 9>>, <<10, 11, 12>>}
      ]

      binary = Encoder.encode_scan_batch(rows)

      assert is_binary(binary)
      {:ok, df} = DF.load_ipc_stream(binary)
      assert DF.n_rows(df) == 2
    end

    test "encodes large batch (1000 rows)" do
      rows = for i <- 1..1000, do: {"key_#{i}", "value_#{i}"}
      binary = Encoder.encode_scan_batch(rows)

      assert is_binary(binary)
      {:ok, df} = DF.load_ipc_stream(binary)
      assert DF.n_rows(df) == 1000
    end
  end

  describe "encode_batch_get_result/1" do
    test "encodes empty batch" do
      results = []
      binary = Encoder.encode_batch_get_result(results)

      assert is_binary(binary)
      {:ok, df} = DF.load_ipc_stream(binary)
      assert DF.n_rows(df) == 0
      # Explorer sorts column names alphabetically
      assert Enum.sort(DF.names(df)) == ["found", "key", "value"]
    end

    test "encodes mixed found/not found results" do
      results = [
        {"k1", "v1", true},
        {"k2", "", false},
        {"k3", "v3", true},
        {"k4", "", false}
      ]

      binary = Encoder.encode_batch_get_result(results)

      assert is_binary(binary)
      {:ok, df} = DF.load_ipc_stream(binary)
      assert DF.n_rows(df) == 4
      assert Enum.sort(DF.names(df)) == ["found", "key", "value"]

      data = DF.to_rows(df)

      assert data == [
               %{"key" => "k1", "value" => "v1", "found" => true},
               %{"key" => "k2", "value" => "", "found" => false},
               %{"key" => "k3", "value" => "v3", "found" => true},
               %{"key" => "k4", "value" => "", "found" => false}
             ]
    end

    test "all results found" do
      results = [
        {"key1", "val1", true},
        {"key2", "val2", true}
      ]

      binary = Encoder.encode_batch_get_result(results)

      {:ok, df} = DF.load_ipc_stream(binary)
      assert DF.n_rows(df) == 2

      # All found should be true
      found_series = DF.pull(df, "found")
      assert Explorer.Series.to_list(found_series) == [true, true]
    end
  end

  describe "performance" do
    @tag :benchmark
    test "encoding performance for 10k rows" do
      rows = for i <- 1..10_000, do: {"key_#{i}", "value_data_#{i}"}

      {time_us, binary} =
        :timer.tc(fn ->
          Encoder.encode_scan_batch(rows)
        end)

      time_ms = time_us / 1000
      rows_per_sec = 10_000 / (time_us / 1_000_000)

      IO.puts("\nEncoding 10k rows:")
      IO.puts("  Time: #{Float.round(time_ms, 2)}ms")
      IO.puts("  Throughput: #{Float.round(rows_per_sec, 0)} rows/sec")
      IO.puts("  Output size: #{byte_size(binary)} bytes")

      # Should be reasonably fast (under 200ms)
      assert time_ms < 500
      assert rows_per_sec > 30_000
    end
  end

  describe "encode_binary_batch/1 and decode_binary_batch/1" do
    test "roundtrip empty list" do
      rows = []
      encoded = Encoder.encode_binary_batch(rows)
      assert <<0::32>> = encoded
      decoded = Encoder.decode_binary_batch(encoded)
      assert decoded == []
    end

    test "roundtrip single row" do
      rows = [{"key1", "value1"}]
      encoded = Encoder.encode_binary_batch(rows)
      decoded = Encoder.decode_binary_batch(encoded)
      assert decoded == rows
    end

    test "roundtrip multiple rows" do
      rows = [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}]
      encoded = Encoder.encode_binary_batch(rows)
      decoded = Encoder.decode_binary_batch(encoded)
      assert decoded == rows
    end

    test "roundtrip binary data" do
      rows = [{<<0, 1, 2>>, <<255, 254, 253>>}]
      encoded = Encoder.encode_binary_batch(rows)
      decoded = Encoder.decode_binary_batch(encoded)
      assert decoded == rows
    end

    test "binary format is more compact than Arrow for small batches" do
      rows = [{"k", "v"}]
      binary_size = byte_size(Encoder.encode_binary_batch(rows))
      arrow_size = byte_size(Encoder.encode_scan_batch(rows))
      # Binary format should be much smaller for tiny data
      assert binary_size < arrow_size
    end
  end
end
