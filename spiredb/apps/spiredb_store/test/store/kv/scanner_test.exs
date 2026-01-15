defmodule Store.KV.ScannerTest do
  use ExUnit.Case, async: true

  alias Store.KV.{Engine}

  setup do
    # Create temp RocksDB
    path = "/tmp/spiredb_test_scanner_#{:rand.uniform(1_000_000)}"
    File.rm_rf!(path)

    {:ok, engine} =
      Engine.start_link(path: path, name: :"test_engine_#{:rand.uniform(1_000_000)}")

    on_exit(fn ->
      # Ensure engine is stopped and release lock
      if Process.alive?(engine) do
        try do
          GenServer.stop(engine, :normal, 5000)
        catch
          :exit, _ -> :ok
        end
      end

      # Clean up files with retry
      File.rm_rf(path)
    end)

    {:ok, engine: engine}
  end

  test "scan returns all rows in range", %{engine: engine} do
    # Insert 100 rows
    for i <- 1..100 do
      key = "key_#{String.pad_leading("#{i}", 3, "0")}"
      value = "value_#{i}"
      :ok = Engine.put(engine, key, value)
    end

    # Scan all
    {:ok, batches} = Engine.scan_range(engine, "key_", "key_~", batch_size: 50)

    all_rows = batches |> Enum.flat_map(fn {rows, _stats} -> rows end)
    assert length(all_rows) == 100
  end

  test "scan respects batch size", %{engine: engine} do
    # Insert 1000 rows
    for i <- 1..1000 do
      :ok = Engine.put(engine, "k#{i}", "v#{i}")
    end

    # Scan with batch_size=100
    {:ok, batches} = Engine.scan_range(engine, "k", "k~", batch_size: 100)

    # Should have 10 batches
    assert length(batches) == 10

    # Each batch should have 100 rows
    for {rows, _stats} <- batches do
      assert length(rows) == 100
    end
  end

  test "scan respects limit", %{engine: engine} do
    # Insert 1000 rows
    for i <- 1..1000 do
      :ok = Engine.put(engine, "key#{i}", "value#{i}")
    end

    # Scan with limit=150
    {:ok, batches} = Engine.scan_range(engine, "key", "key~", batch_size: 50, limit: 150)

    all_rows = batches |> Enum.flat_map(fn {rows, _stats} -> rows end)
    assert length(all_rows) == 150
  end

  test "scan returns stats", %{engine: engine} do
    # Insert rows
    for i <- 1..10 do
      :ok = Engine.put(engine, "test#{i}", "data#{i}")
    end

    {:ok, batches} = Engine.scan_range(engine, "test", "test~")

    for {_rows, stats} <- batches do
      assert stats.rows_returned > 0
      assert stats.bytes_read > 0
      assert stats.scan_time_ms >= 0
    end
  end

  test "empty range returns empty result", %{engine: engine} do
    {:ok, batches} = Engine.scan_range(engine, "nonexistent", "nonexistent~")
    assert batches == []
  end
end
