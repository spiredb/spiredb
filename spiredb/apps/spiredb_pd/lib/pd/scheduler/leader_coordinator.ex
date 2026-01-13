defmodule PD.Scheduler.LeaderCoordinator do
  @moduledoc """
  Global leader election coordination.

  Ensures balanced leader distribution across stores and handles
  coordinated leader transfers when stores join/leave the cluster.
  """

  require Logger

  alias PD.Server

  @doc """
  Balance leaders across all stores.

  Calculates desired leader distribution and schedules transfers
  to achieve balance.

  Returns list of transfer operations to execute.
  """
  @spec balance_leaders() :: {:ok, [map()]} | {:error, term()}
  def balance_leaders do
    with {:ok, stores} <- Server.get_all_stores(),
         {:ok, regions} <- Server.get_all_regions() do
      active_stores = Enum.filter(stores, &(&1.state == :up))

      if length(active_stores) == 0 do
        {:ok, []}
      else
        transfers = calculate_leader_transfers(active_stores, regions)
        {:ok, transfers}
      end
    end
  end

  @doc """
  Transfer leadership for a region to a specific store.
  """
  @spec transfer_leader(non_neg_integer(), atom()) :: :ok | {:error, term()}
  def transfer_leader(region_id, target_store) do
    Logger.info("Initiating leader transfer: region=#{region_id} target=#{target_store}")

    case Server.get_region_by_id(region_id) do
      {:ok, region} when region != nil ->
        if target_store in (region.stores || []) do
          # Region store has the replica - can transfer
          do_transfer(region_id, region.leader, target_store)
        else
          {:error, :target_not_replica}
        end

      {:ok, nil} ->
        {:error, :region_not_found}

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Elect new leader for a region after current leader failure.
  Chooses the store with lowest leader count.
  """
  @spec elect_new_leader(non_neg_integer()) :: {:ok, atom()} | {:error, term()}
  def elect_new_leader(region_id) do
    with {:ok, region} <- Server.get_region_by_id(region_id),
         {:ok, stores} <- Server.get_all_stores() do
      # Filter to stores that have this region
      eligible =
        stores
        |> Enum.filter(&(&1.state == :up and &1.node in (region.stores || [])))

      case eligible do
        [] ->
          {:error, :no_eligible_stores}

        candidates ->
          # Pick store with lowest current leader count
          {:ok, all_regions} = Server.get_all_regions()
          leader_counts = count_leaders_per_store(all_regions)

          chosen =
            Enum.min_by(candidates, fn store ->
              Map.get(leader_counts, store.node, 0)
            end)

          Logger.info("Elected new leader for region #{region_id}: #{chosen.node}")
          {:ok, chosen.node}
      end
    end
  end

  @doc """
  Handle store going down - trigger leader elections for affected regions.
  """
  @spec handle_store_down(atom()) :: :ok
  def handle_store_down(store_node) do
    Logger.warning("Store down: #{store_node}, checking leader elections")

    case Server.get_all_regions() do
      {:ok, regions} ->
        # Find regions where this store was leader
        affected = Enum.filter(regions, &(&1.leader == store_node))

        Enum.each(affected, fn region ->
          case elect_new_leader(region.id) do
            {:ok, new_leader} ->
              Logger.info("Region #{region.id} new leader: #{new_leader}")

            # Update PD with new leader info would happen via Raft

            {:error, reason} ->
              Logger.error("Failed to elect leader for region #{region.id}: #{reason}")
          end
        end)

      {:error, _} ->
        :ok
    end

    :ok
  end

  ## Private

  defp calculate_leader_transfers(stores, regions) do
    leader_counts = count_leaders_per_store(regions)
    num_stores = length(stores)
    num_regions = length(regions)

    # Ideal distribution
    ideal = div(num_regions, max(num_stores, 1))
    max_leaders = ideal + 1

    # Find overloaded and underloaded stores
    store_nodes = Enum.map(stores, & &1.node)

    overloaded =
      Enum.filter(store_nodes, fn node ->
        Map.get(leader_counts, node, 0) > max_leaders
      end)

    underloaded =
      Enum.filter(store_nodes, fn node ->
        Map.get(leader_counts, node, 0) < ideal
      end)

    # Generate transfer operations
    generate_transfers(regions, overloaded, underloaded, leader_counts, [])
  end

  defp generate_transfers(_regions, [], _underloaded, _counts, transfers), do: transfers
  defp generate_transfers(_regions, _overloaded, [], _counts, transfers), do: transfers

  defp generate_transfers(regions, [over | rest_over], [under | rest_under], counts, transfers) do
    # Find a region led by overloaded store that can be transferred
    case find_transferable_region(regions, over, under) do
      nil ->
        generate_transfers(regions, rest_over, [under | rest_under], counts, transfers)

      region ->
        transfer = %{
          type: :transfer_leader,
          region_id: region.id,
          from_store: over,
          to_store: under
        }

        new_counts =
          counts
          |> Map.update(over, 0, &(&1 - 1))
          |> Map.update(under, 0, &(&1 + 1))

        # Check if stores are still over/under loaded
        new_over =
          if Map.get(new_counts, over, 0) >
               div(length(regions), length([over | rest_over] ++ [under | rest_under])) + 1,
             do: [over | rest_over],
             else: rest_over

        new_under =
          if Map.get(new_counts, under, 0) <
               div(length(regions), length([over | rest_over] ++ [under | rest_under])),
             do: [under | rest_under],
             else: rest_under

        generate_transfers(regions, new_over, new_under, new_counts, [transfer | transfers])
    end
  end

  defp find_transferable_region(regions, from_store, to_store) do
    Enum.find(regions, fn region ->
      region.leader == from_store and to_store in (region.stores || [])
    end)
  end

  defp count_leaders_per_store(regions) do
    Enum.reduce(regions, %{}, fn region, acc ->
      if region.leader do
        Map.update(acc, region.leader, 1, &(&1 + 1))
      else
        acc
      end
    end)
  end

  defp do_transfer(region_id, from_store, target_store) do
    # Compute server_id locally (format: {region_id, node})
    # This avoids cross-app dependency on Store.Region.Raft
    server_id = {region_id, from_store}
    target_server_id = {region_id, target_store}

    try do
      case :ra.transfer_leadership(server_id, target_server_id) do
        :ok -> :ok
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
        {:timeout, _} -> {:error, :timeout}
      end
    catch
      :exit, reason -> {:error, {:exit, reason}}
    end
  end
end
