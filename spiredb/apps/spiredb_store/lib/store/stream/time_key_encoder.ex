defmodule Store.Stream.TimeKeyEncoder do
  @moduledoc """
  Encode keys with timestamp prefix for time-ordered storage.

  Format: {table_id:4B}{timestamp:8B reverse}{entity_id}

  Uses reverse timestamp (max_int64 - ts) for newest-first ordering.
  """

  @max_int64 0xFFFFFFFFFFFFFFFF

  def encode(table_id, timestamp, entity_id)
      when is_integer(table_id) and is_integer(timestamp) do
    reverse_ts = @max_int64 - timestamp
    <<table_id::32, reverse_ts::64, entity_id::binary>>
  end

  def decode(<<table_id::32, reverse_ts::64, entity_id::binary>>) do
    timestamp = @max_int64 - reverse_ts
    {:ok, table_id, timestamp, entity_id}
  end

  def decode(_) do
    {:error, :invalid_key}
  end

  @doc """
  Calculate key range for a time window (from_ts is older, to_ts is newer).
  Because we scan via RocksDB (usually forward), and we use reverse timestamp:
  - Newer items (larger ts) have SMALLER keys (smaller reverse_ts).
  - Older items (smaller ts) have LARGER keys (larger reverse_ts).

  So to scan newest-first (descending time): start with to_ts (newer) and iterate forward.
  Wait, RocksDB sort is bytewise ascending.
  Smallest buffer (Newest item) -> Largest buffer (Oldest item).

  Range [to_ts, from_ts] in time means [start_key, end_key] in bytewise order.
  """
  def range_for_time_window(table_id, from_ts, to_ts) do
    start_rev = @max_int64 - to_ts
    end_rev = @max_int64 - from_ts

    start_key = <<table_id::32, start_rev::64>>
    # End key needs to cover the entire end_rev, so append 0xFF or similar logic
    # Actually just end_rev itself? No, end_rev + entity_id > end_rev
    # So we need end_rev + highest byte?
    # Or just return prefix?
    # Usually we want inclusive range.
    end_key = <<table_id::32, end_rev::64, 0xFF, 0xFF, 0xFF, 0xFF>>

    {start_key, end_key}
  end
end
