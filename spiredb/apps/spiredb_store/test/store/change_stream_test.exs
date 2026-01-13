defmodule Store.ChangeStreamTest do
  use ExUnit.Case, async: false

  alias Store.ChangeStream

  alias Store.Test.RocksDBHelper

  setup do
    # Setup isolated RocksDB for this test
    {:ok, _db, _cfs} = RocksDBHelper.setup_rocksdb("change_stream_test")

    # Check if ChangeStream is running
    unless Process.whereis(ChangeStream) do
      start_supervised!({ChangeStream, name: ChangeStream})
    end

    :ok
  end

  describe "record_change/4" do
    test "records a put change" do
      # Get initial seq
      {:ok, initial_seq} = ChangeStream.current_seq()

      # Record a change
      :ok = ChangeStream.record_change(:put, "default", "test_key", "test_value")

      # Wait for async processing
      Process.sleep(50)

      # Seq should have incremented
      {:ok, new_seq} = ChangeStream.current_seq()
      assert new_seq > initial_seq
    end

    test "records a delete change" do
      {:ok, initial_seq} = ChangeStream.current_seq()

      :ok = ChangeStream.record_change(:delete, "default", "deleted_key", nil)

      Process.sleep(50)

      {:ok, new_seq} = ChangeStream.current_seq()
      assert new_seq > initial_seq
    end
  end

  describe "get_changes/2" do
    test "retrieves changes since sequence" do
      {:ok, start_seq} = ChangeStream.current_seq()

      # Record some changes
      ChangeStream.record_change(:put, "default", "key1", "value1")
      ChangeStream.record_change(:put, "default", "key2", "value2")
      ChangeStream.record_change(:delete, "default", "key3", nil)

      Process.sleep(50)

      {:ok, changes, last_seq} = ChangeStream.get_changes(start_seq)

      assert length(changes) >= 3
      assert last_seq > start_seq

      # Check change structure
      [first | _] = changes
      assert Map.has_key?(first, :seq)
      assert Map.has_key?(first, :type)
      assert Map.has_key?(first, :key)
      assert Map.has_key?(first, :timestamp)
    end

    test "filters by column family" do
      {:ok, start_seq} = ChangeStream.current_seq()

      ChangeStream.record_change(:put, "default", "def_key", "value")
      ChangeStream.record_change(:put, "tables", "tbl_key", "value")

      Process.sleep(50)

      {:ok, default_changes, _} = ChangeStream.get_changes(start_seq, cf: "default")
      {:ok, table_changes, _} = ChangeStream.get_changes(start_seq, cf: "tables")

      # Filter should work
      default_cfs = Enum.map(default_changes, & &1.cf) |> Enum.uniq()
      table_cfs = Enum.map(table_changes, & &1.cf) |> Enum.uniq()

      assert default_cfs == [] or default_cfs == ["default"]
      assert table_cfs == [] or table_cfs == ["tables"]
    end

    test "respects limit option" do
      {:ok, start_seq} = ChangeStream.current_seq()

      # Record many changes
      for i <- 1..10 do
        ChangeStream.record_change(:put, "default", "key#{i}", "value#{i}")
      end

      Process.sleep(100)

      {:ok, changes, _} = ChangeStream.get_changes(start_seq, limit: 5)

      assert length(changes) <= 5
    end
  end

  describe "subscribe/1 and unsubscribe/0" do
    test "receives changes via subscription" do
      :ok = ChangeStream.subscribe(from: :latest)

      # Record a change
      ChangeStream.record_change(:put, "default", "sub_key", "sub_value")

      # Should receive the change
      assert_receive {:change, change}, 500
      assert change.key == "sub_key"
      assert change.type == :put

      :ok = ChangeStream.unsubscribe()
    end

    test "stops receiving after unsubscribe" do
      :ok = ChangeStream.subscribe(from: :latest)
      :ok = ChangeStream.unsubscribe()

      ChangeStream.record_change(:put, "default", "no_sub_key", "value")

      refute_receive {:change, _}, 100
    end
  end

  describe "current_seq/0" do
    test "returns current sequence number" do
      {:ok, seq} = ChangeStream.current_seq()

      assert is_integer(seq)
      assert seq >= 0
    end
  end

  describe "offset-based subscription" do
    test "subscribe_from_offset/3 resumes from offset" do
      consumer_id = "test_consumer_1"
      {:ok, initial_seq} = ChangeStream.current_seq()

      # Record a change
      ChangeStream.record_change(:put, "default", "resume_key", "value")
      Process.sleep(50)
      {:ok, seq} = ChangeStream.current_seq()
      assert seq > initial_seq

      # Subscribe from *before* the change (initial_seq is old seq)
      # If current seq is 101, and we subscribe from 100, we should get 101.
      :ok = ChangeStream.subscribe_from_offset(consumer_id, initial_seq, [])

      # Should receive the change
      assert_receive {:change, change}, 500
      assert change.seq == seq
      assert change.key == "resume_key"

      ChangeStream.unsubscribe()
    end

    test "acknowledge/2 stores offset" do
      consumer_id = "test_consumer_2"
      offset = 12345

      :ok = ChangeStream.acknowledge(consumer_id, offset)
      # Allow async write
      Process.sleep(50)

      # Verify stored offset using ChangeStream API
      assert {:ok, ^offset} = ChangeStream.get_consumer_offset(consumer_id)
    end
  end
end
