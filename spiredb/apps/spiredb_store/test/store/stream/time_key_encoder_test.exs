defmodule Store.Stream.TimeKeyEncoderTest do
  use ExUnit.Case, async: true
  alias Store.Stream.TimeKeyEncoder

  test "encodes and decodes keys" do
    table_id = 1
    ts = 1_000_000
    entity_id = "entity1"

    key = TimeKeyEncoder.encode(table_id, ts, entity_id)
    assert byte_size(key) == 4 + 8 + byte_size(entity_id)

    assert {:ok, ^table_id, ^ts, ^entity_id} = TimeKeyEncoder.decode(key)
  end

  test "sort order is newest first" do
    table_id = 1
    older_ts = 1_000_000
    newer_ts = 2_000_000

    key_older = TimeKeyEncoder.encode(table_id, older_ts, "a")
    key_newer = TimeKeyEncoder.encode(table_id, newer_ts, "a")

    # In RocksDB (bytewise sort), smaller buffer comes first.
    # We want newest first.
    # Reverse ts: max - ts.
    # max - 2_000_000 < max - 1_000_000
    # So key_newer < key_older

    assert key_newer < key_older
  end

  test "calculates range for time window" do
    table_id = 1
    from_ts = 1_000_000
    to_ts = 2_000_000

    {start_key, end_key} = TimeKeyEncoder.range_for_time_window(table_id, from_ts, to_ts)

    # Newer (to_ts) should be start (smaller key)
    # Older (from_ts) should be end (larger key)

    key_in_range = TimeKeyEncoder.encode(table_id, 1_500_000, "x")
    key_newer_out = TimeKeyEncoder.encode(table_id, 2_100_000, "x")
    key_older_out = TimeKeyEncoder.encode(table_id, 500_000, "x")

    assert start_key <= key_in_range
    assert key_in_range <= end_key

    # key_newer uses smaller reverse_ts -> smaller than start_key
    assert key_newer_out < start_key

    # key_older uses larger reverse_ts -> larger than end_key (hopefully?)
    # end_key has 0xFF padding on end_rev
    # key_older has end_rev_older + suffix
    # end_rev_older > end_rev
    assert key_older_out > end_key
  end
end
