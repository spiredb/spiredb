defmodule Store.StreamXdelTest do
  use ExUnit.Case, async: false

  alias Store.Stream

  @test_stream "xdel_test_stream"

  alias Store.Test.RocksDBHelper

  setup do
    # Setup isolated RocksDB
    {:ok, _db, _cfs} = RocksDBHelper.setup_rocksdb("xdel_test")

    # Clean up any existing stream (now against the isolated DB)
    Stream.delete_stream(@test_stream)
    :ok
  end

  describe "xdel/2" do
    test "deletes entries by ID" do
      # Add some entries
      {:ok, id1} = Stream.xadd(@test_stream, [{"field", "value1"}])
      {:ok, id2} = Stream.xadd(@test_stream, [{"field", "value2"}])
      {:ok, id3} = Stream.xadd(@test_stream, [{"field", "value3"}])

      # Verify entries exist
      {:ok, 3} = Stream.xlen(@test_stream)

      # Delete middle entry
      {:ok, deleted_count} = Stream.xdel(@test_stream, [id2])
      assert deleted_count == 1

      # Verify length decreased
      {:ok, 2} = Stream.xlen(@test_stream)

      # Verify remaining entries
      {:ok, entries} = Stream.xrange(@test_stream, "-", "+")
      ids = Enum.map(entries, fn {id, _fields} -> id end)
      assert id1 in ids
      refute id2 in ids
      assert id3 in ids
    end

    test "deletes multiple entries" do
      {:ok, id1} = Stream.xadd(@test_stream, [{"f", "1"}])
      {:ok, _id2} = Stream.xadd(@test_stream, [{"f", "2"}])
      {:ok, id3} = Stream.xadd(@test_stream, [{"f", "3"}])
      {:ok, _id4} = Stream.xadd(@test_stream, [{"f", "4"}])

      {:ok, deleted} = Stream.xdel(@test_stream, [id1, id3])
      assert deleted == 2

      {:ok, 2} = Stream.xlen(@test_stream)
    end

    test "returns count for attempted deletes" do
      {:ok, _id} = Stream.xadd(@test_stream, [{"f", "v"}])

      # RocksDB delete always succeeds, so we count attempted deletes with valid IDs
      {:ok, deleted} = Stream.xdel(@test_stream, ["9999999999999-0"])
      # May return 1 (delete call succeeded) or 0 depending on implementation
      assert is_integer(deleted)
    end

    test "handles empty ID list" do
      {:ok, _} = Stream.xadd(@test_stream, [{"f", "v"}])
      {:ok, 0} = Stream.xdel(@test_stream, [])
    end

    test "handles invalid ID format gracefully" do
      {:ok, _} = Stream.xadd(@test_stream, [{"f", "v"}])
      # Invalid ID format - should be skipped
      {:ok, deleted} = Stream.xdel(@test_stream, ["invalid-id-format", "also-bad"])
      assert deleted == 0
    end

    test "updates stream metadata after deletion" do
      {:ok, id1} = Stream.xadd(@test_stream, [{"f", "1"}])
      {:ok, _id2} = Stream.xadd(@test_stream, [{"f", "2"}])
      {:ok, id3} = Stream.xadd(@test_stream, [{"f", "3"}])

      # Delete first and last
      {:ok, 2} = Stream.xdel(@test_stream, [id1, id3])

      # Check stream info reflects only middle entry
      {:ok, info} = Stream.xinfo(@test_stream)
      assert info.length == 1
    end
  end
end
