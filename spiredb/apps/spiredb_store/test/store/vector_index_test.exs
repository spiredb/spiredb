defmodule Store.VectorIndexTest do
  @moduledoc """
  Tests for vector index operations.
  """

  use ExUnit.Case, async: false

  alias Store.Test.RocksDBHelper
  alias Store.VectorIndex

  @pid :test_vector_index

  setup do
    # Setup test-specific RocksDB
    {:ok, _db, _cfs} = RocksDBHelper.setup_rocksdb("vector_index_test")

    # Use a unique data directory for this test run
    data_dir = "/tmp/spiredb_test_vectors_#{System.unique_integer([:positive])}"
    File.rm_rf!(data_dir)

    # Start a fresh VectorIndex process for this test
    # We use a unique name (@pid) to avoid clashing with the global Store.VectorIndex
    start_supervised!({Store.VectorIndex, name: @pid, data_dir: data_dir})

    :ok
  end

  describe "index lifecycle" do
    test "creates index" do
      {:ok, id} =
        VectorIndex.create_index(@pid, "test_idx", "table1", "embedding",
          algorithm: :anode,
          dimensions: 128
        )

      assert is_integer(id)
    end

    test "prevents duplicate index names" do
      {:ok, _} = VectorIndex.create_index(@pid, "dup_idx", "t1", "c1", dimensions: 64)
      result = VectorIndex.create_index(@pid, "dup_idx", "t2", "c2", dimensions: 64)

      assert result == {:error, :already_exists}
    end

    test "drops index" do
      {:ok, _} = VectorIndex.create_index(@pid, "drop_idx", "table", "col", dimensions: 32)

      assert :ok = VectorIndex.drop_index(@pid, "drop_idx")
      assert {:error, :not_found} = VectorIndex.drop_index(@pid, "drop_idx")
    end

    test "lists indexes" do
      {:ok, _} = VectorIndex.create_index(@pid, "idx1", "t1", "c1", dimensions: 64)
      {:ok, _} = VectorIndex.create_index(@pid, "idx2", "t2", "c2", dimensions: 64)

      {:ok, indexes} = VectorIndex.list_indexes(@pid)

      names = Enum.map(indexes, & &1.name)
      assert "idx1" in names
      assert "idx2" in names
    end
  end

  describe "vector operations" do
    setup do
      {:ok, _} =
        VectorIndex.create_index(@pid, "vec_idx", "docs", "embedding",
          algorithm: :anode,
          dimensions: 4
        )

      :ok
    end

    test "inserts vector" do
      vector = [1.0, 2.0, 3.0, 4.0]
      {:ok, internal_id} = VectorIndex.insert(@pid, "vec_idx", "doc:1", vector)

      assert is_integer(internal_id)
    end

    test "inserts vector with payload" do
      vector = [1.0, 0.0, 0.0, 0.0]
      payload = ~s({"title": "Test Document"})

      {:ok, _} = VectorIndex.insert(@pid, "vec_idx", "doc:2", vector, payload)
    end

    test "deletes vector" do
      vector = [1.0, 1.0, 1.0, 1.0]
      {:ok, _} = VectorIndex.insert(@pid, "vec_idx", "doc:3", vector)

      assert :ok = VectorIndex.delete(@pid, "vec_idx", "doc:3")
    end

    test "returns error for unknown index" do
      assert {:error, :index_not_found} = VectorIndex.insert(@pid, "unknown", "doc", [1.0])
      assert {:error, :index_not_found} = VectorIndex.search(@pid, "unknown", [1.0], 5)
    end
  end

  describe "vector search" do
    setup do
      {:ok, _} =
        VectorIndex.create_index(@pid, "search_idx", "docs", "vec",
          algorithm: :anode,
          dimensions: 3
        )

      VectorIndex.insert(@pid, "search_idx", "doc:a", [1.0, 0.0, 0.0], ~s({"label":"A"}))
      VectorIndex.insert(@pid, "search_idx", "doc:b", [0.0, 1.0, 0.0], ~s({"label":"B"}))
      VectorIndex.insert(@pid, "search_idx", "doc:c", [0.0, 0.0, 1.0], ~s({"label":"C"}))
      VectorIndex.insert(@pid, "search_idx", "doc:d", [1.0, 1.0, 0.0], ~s({"label":"D"}))

      :ok
    end

    test "finds nearest neighbors" do
      query = [1.0, 0.0, 0.0]
      # Use wait_for_results to handle CI timing
      {:ok, results} = wait_for_results(@pid, "search_idx", query, 2, expected_count: 2)

      [{first_id, first_dist, _} | _] = results
      assert first_id == "doc:a"
      assert first_dist < 0.1
    end

    test "returns payloads when requested" do
      query = [0.0, 1.0, 0.0]

      {:ok, results} =
        wait_for_results(@pid, "search_idx", query, 1, return_payload: true, expected_count: 1)

      [{_id, _dist, payload}] = results
      assert payload != nil
      assert String.contains?(payload, "B")
    end

    test "respects k limit" do
      query = [0.5, 0.5, 0.0]
      {:ok, results} = wait_for_results(@pid, "search_idx", query, 3, expected_count: 3)
      assert length(results) == 3
    end

    test "results sorted by distance" do
      query = [1.0, 0.0, 0.0]
      {:ok, results} = wait_for_results(@pid, "search_idx", query, 4, expected_count: 4)

      distances = Enum.map(results, fn {_, d, _} -> d end)
      assert distances == Enum.sort(distances)
    end
  end

  describe "binary vector handling" do
    setup do
      {:ok, _} = VectorIndex.create_index(@pid, "bin_idx", "t", "c", dimensions: 2)
      :ok
    end

    test "handles float32 binary vectors" do
      bin_vector = <<1.0::float-32-native, 2.0::float-32-native>>

      {:ok, _} = VectorIndex.insert(@pid, "bin_idx", "bin:1", bin_vector)

      query = <<1.0::float-32-native, 2.0::float-32-native>>

      # Wait for the result to appear
      {:ok, results} = wait_for_results(@pid, "bin_idx", query, 1, expected_count: 1)

      [{id, dist, _}] = results
      assert id == "bin:1"
      assert dist < 0.01
    end
  end

  # Helper to poll for search results until they appear or timeout
  # Essential for CI where async indexing may be slower
  defp wait_for_results(pid, index, query, k, opts) do
    expected_count = Keyword.get(opts, :expected_count, 1)
    # Remove our test-specific opt before passing to search
    search_opts = Keyword.delete(opts, :expected_count)

    # If nil (timed out), make one last call to return the actual result/error for assertion
    Stream.unfold(0, fn
      # Timeout after 20 attempts * 100ms = 2s
      20 ->
        nil

      n ->
        case VectorIndex.search(pid, index, query, k, search_opts) do
          {:ok, results} when length(results) >= expected_count ->
            # Found enough results
            # Done
            {{:ok, results}, 20}

          # Not enough results or error, wait and retry
          _ ->
            Process.sleep(100)
            {nil, n + 1}
        end
    end)
    |> Enum.take(1)
    |> List.first() ||
      VectorIndex.search(pid, index, query, k, search_opts)
  end
end
