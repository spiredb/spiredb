defmodule Store.VectorSearch.Distributed do
  @moduledoc """
  Distributed vector search across multiple regions.

  Fans out vector search queries to all stores hosting vector indexes,
  collects results, and merges them by distance to return top-k globally.
  """

  require Logger

  alias Store.VectorIndex

  @search_timeout 30_000
  @default_k 10

  @doc """
  Execute a distributed vector search across all stores.

  Queries each store in parallel and merges results by distance.

  ## Options
  - `:k` - Number of results to return (default: 10)
  - `:return_payload` - Include payloads in results (default: false)
  - `:timeout` - Query timeout in ms (default: 30000)
  """
  @spec search(String.t(), [float()], keyword()) :: {:ok, list()} | {:error, term()}
  def search(index_name, query_vector, opts \\ []) do
    k = Keyword.get(opts, :k, @default_k)
    timeout = Keyword.get(opts, :timeout, @search_timeout)

    # Get all stores from PD
    case get_all_stores() do
      {:ok, stores} ->
        # Fan out queries to all stores in parallel
        results = query_all_stores(stores, index_name, query_vector, k, opts, timeout)

        # Merge and return top-k globally
        merged = merge_results(results, k)
        {:ok, merged}

      {:error, _} = err ->
        # Fallback to local search
        Logger.warning("Distributed search fallback to local: #{inspect(err)}")

        try do
          VectorIndex.search(index_name, query_vector, k, opts)
        catch
          :exit, reason -> {:error, {:local_search_failed, reason}}
        end
    end
  end

  @doc """
  Execute a search on specific stores only.
  Useful for targeted queries when you know which stores have the data.
  """
  @spec search_stores([atom()], String.t(), [float()], keyword()) ::
          {:ok, list()} | {:error, term()}
  def search_stores(store_nodes, index_name, query_vector, opts \\ []) do
    k = Keyword.get(opts, :k, @default_k)
    timeout = Keyword.get(opts, :timeout, @search_timeout)

    stores = Enum.map(store_nodes, fn node -> %{node: node, state: :up} end)
    results = query_all_stores(stores, index_name, query_vector, k, opts, timeout)
    merged = merge_results(results, k)
    {:ok, merged}
  end

  ## Private

  defp get_all_stores do
    try do
      PD.Server.get_all_stores()
    catch
      :exit, _ -> {:error, :pd_unavailable}
    end
  end

  defp query_all_stores(stores, index_name, query_vector, k, opts, timeout) do
    # Filter to active stores
    active_stores = Enum.filter(stores, &(&1.state == :up))

    # Start async tasks for each store
    tasks =
      Enum.map(active_stores, fn store ->
        Task.async(fn ->
          query_store(store.node, index_name, query_vector, k, opts)
        end)
      end)

    # Await all with timeout
    Task.await_many(tasks, timeout)
    |> Enum.flat_map(fn
      {:ok, results} -> results
      {:error, _} -> []
    end)
  end

  defp query_store(store_node, index_name, query_vector, k, opts) do
    if store_node == node() do
      # Local query
      try do
        VectorIndex.search(index_name, query_vector, k, opts)
      rescue
        e -> {:error, {:exception, Exception.message(e)}}
      catch
        :exit, reason -> {:error, {:exit, reason}}
      end
    else
      # Remote query via RPC
      try do
        case :rpc.call(
               store_node,
               VectorIndex,
               :search,
               [index_name, query_vector, k, opts],
               10_000
             ) do
          {:ok, results} -> {:ok, results}
          {:error, _} = err -> err
          {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
        end
      catch
        :exit, reason -> {:error, {:exit, reason}}
      end
    end
  end

  defp merge_results(results, k) do
    # Results are tuples of {doc_id, distance, payload}
    # Sort by distance (ascending) and take top k
    results
    |> Enum.sort_by(fn
      {_doc_id, distance, _payload} -> distance
      {_doc_id, distance} -> distance
    end)
    |> Enum.take(k)
    |> Enum.uniq_by(fn
      {doc_id, _, _} -> doc_id
      {doc_id, _} -> doc_id
    end)
  end
end
