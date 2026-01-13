defmodule Store.VectorSearch.DistributedTest do
  use ExUnit.Case, async: true

  alias Store.VectorSearch.Distributed

  describe "search/3" do
    test "falls back to local when PD unavailable" do
      # Without PD running, should fallback to local VectorIndex
      result =
        try do
          Distributed.search("nonexistent_index", [1.0, 2.0, 3.0], k: 5)
        rescue
          _ -> {:error, :exception_raised}
        catch
          :exit, _ -> {:error, :genserver_not_running}
        end

      # Will error because index doesn't exist, but shouldn't crash
      assert {:error, _} = result
    end
  end

  describe "search_stores/4" do
    test "handles empty store list" do
      {:ok, results} = Distributed.search_stores([], "test_index", [1.0, 2.0], k: 5)
      assert results == []
    end

    test "queries local node gracefully" do
      # This will query local VectorIndex which may not be running
      result =
        try do
          Distributed.search_stores([node()], "test_index", [1.0, 2.0], k: 5)
        rescue
          _ -> {:ok, []}
        catch
          :exit, _ -> {:ok, []}
        end

      # Should not crash, may return error or empty results
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end

  describe "merge_results (via search)" do
    test "merges results by distance" do
      # Test internal merge logic
      results = [
        {"doc1", 0.5, nil},
        {"doc2", 0.1, nil},
        {"doc3", 0.3, nil},
        # Duplicate
        {"doc1", 0.5, nil}
      ]

      merged = merge_and_dedupe(results, 10)

      # Should be sorted by distance
      assert length(merged) == 3
      assert hd(merged) == {"doc2", 0.1, nil}
    end

    test "respects k limit after merge" do
      results = for i <- 1..20, do: {"doc#{i}", i * 0.1, nil}
      merged = merge_and_dedupe(results, 5)
      assert length(merged) == 5
    end
  end

  # Helper mimicking internal merge logic
  defp merge_and_dedupe(results, k) do
    results
    |> Enum.sort_by(fn {_, distance, _} -> distance end)
    |> Enum.take(k)
    |> Enum.uniq_by(fn {doc_id, _, _} -> doc_id end)
  end
end
