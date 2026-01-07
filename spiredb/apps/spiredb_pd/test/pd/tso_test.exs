defmodule PD.TSOTest do
  @moduledoc """
  Tests for Timestamp Oracle.
  """

  use ExUnit.Case, async: false

  alias PD.TSO

  setup do
    start_supervised!({TSO, name: TSO})
    :ok
  end

  describe "timestamp allocation" do
    test "allocates single timestamp" do
      {:ok, ts1} = TSO.get_timestamp()
      {:ok, ts2} = TSO.get_timestamp()

      assert is_integer(ts1)
      assert is_integer(ts2)
      assert ts2 > ts1
    end

    test "allocates timestamps in batch" do
      {:ok, start_ts, count} = TSO.get_timestamps(100)

      assert is_integer(start_ts)
      assert count == 100
    end

    test "batch timestamps are contiguous" do
      {:ok, start1, count1} = TSO.get_timestamps(50)
      {:ok, start2, _count2} = TSO.get_timestamps(50)

      # Second batch should start after first batch ends
      assert start2 >= start1 + count1
    end

    test "current returns current without allocating" do
      {:ok, before} = TSO.current()
      {:ok, ts} = TSO.get_timestamp()
      {:ok, after_ts} = TSO.current()

      assert ts >= before
      assert after_ts >= ts
    end

    test "timestamps are monotonically increasing across many calls" do
      timestamps =
        for _ <- 1..1000 do
          {:ok, ts} = TSO.get_timestamp()
          ts
        end

      # All timestamps should be unique and increasing
      assert timestamps == Enum.uniq(timestamps)
      assert timestamps == Enum.sort(timestamps)
    end
  end

  describe "concurrent access" do
    test "handles concurrent timestamp requests" do
      tasks =
        for _ <- 1..100 do
          Task.async(fn ->
            {:ok, ts} = TSO.get_timestamp()
            ts
          end)
        end

      timestamps = Task.await_many(tasks)

      # All should be unique
      assert length(Enum.uniq(timestamps)) == 100
    end

    test "handles concurrent batch requests" do
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            {:ok, start, count} = TSO.get_timestamps(100)
            {start, count}
          end)
        end

      batches = Task.await_many(tasks)

      # Batches should not overlap
      ranges =
        Enum.map(batches, fn {start, count} ->
          MapSet.new(start..(start + count - 1))
        end)

      for {r1, i} <- Enum.with_index(ranges),
          {r2, j} <- Enum.with_index(ranges),
          i < j do
        assert MapSet.disjoint?(r1, r2), "Batches #{i} and #{j} overlap"
      end
    end
  end
end
