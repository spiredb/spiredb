defmodule PD.API.GRPC.Cluster do
  @moduledoc """
  gRPC ClusterService implementation for Placement Driver.
  Provides region discovery and store management.
  """

  use GRPC.Server, service: Spiredb.Cluster.ClusterService.Service

  require Logger
  alias PD.Server

  alias Spiredb.Cluster.{
    Region,
    RegionList,
    StoreList,
    RegisterStoreResponse,
    StoreHeartbeatResponse,
    Peer
  }

  @doc """
  Get region by ID.
  """
  def get_region(request, _stream) do
    Logger.debug("GetRegion", region_id: request.region_id)

    case Server.get_region_by_id(request.region_id) do
      {:ok, nil} ->
        raise GRPC.RPCError, status: :not_found, message: "Region not found"

      {:ok, region} ->
        region_to_proto(region)

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Region not found"

      {:error, reason} ->
        Logger.error("GetRegion failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  @doc """
  Get region by key.
  """
  def get_region_by_key(request, _stream) do
    Logger.debug("GetRegionByKey", key: request.key)

    case Server.find_region(request.key) do
      {:ok, nil} ->
        raise GRPC.RPCError, status: :not_found, message: "Region not found"

      {:ok, region} ->
        region_to_proto(region)

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Region not found"

      {:error, reason} ->
        Logger.error("GetRegionByKey failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  @doc """
  Get all regions for a table.
  """
  def get_table_regions(request, _stream) do
    Logger.debug("GetTableRegions", table: request.table_name)

    # 1. Get region IDs from Schema Registry
    case PD.Schema.Registry.get_table_regions(request.table_name) do
      {:ok, region_ids} ->
        # 2. Fetch full region details from PD Server
        regions =
          region_ids
          |> Enum.map(fn id ->
            case Server.get_region_by_id(id) do
              {:ok, region} -> region
              _ -> nil
            end
          end)
          |> Enum.reject(&is_nil/1)

        %RegionList{regions: Enum.map(regions, &region_to_proto/1)}

      {:error, :not_found} ->
        # Return empty list if table not found (or raise not found?)
        # SpireSQL expects empty list if no regions yet
        %RegionList{regions: []}

      {:error, reason} ->
        Logger.error("GetTableRegions failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  @doc """
  Get store by ID.
  """
  def get_store(request, _stream) do
    case Server.get_store_by_id(request.store_id) do
      {:ok, nil} ->
        raise GRPC.RPCError, status: :not_found, message: "Store not found"

      {:ok, store} ->
        store_to_proto(store)

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "Store not found"

      {:error, reason} ->
        Logger.error("GetStore failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  @doc """
  List all stores.
  """
  def list_stores(_request, _stream) do
    case Server.get_all_stores() do
      {:ok, stores} ->
        proto_stores = Enum.map(stores, &store_to_proto/1)
        %StoreList{stores: proto_stores}

      {:error, reason} ->
        Logger.error("ListStores failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  defp store_to_proto(store) do
    store_id = if is_atom(store.node), do: :erlang.phash2(store.node), else: store.node
    address = if is_atom(store.node), do: Atom.to_string(store.node), else: "#{store.node}"

    region_count = length(store.regions || [])
    state = if store.state == :up, do: :STORE_UP, else: :STORE_DOWN

    %Spiredb.Cluster.Store{
      id: store_id,
      address: address,
      state: state,
      capacity: 0,
      available: 0,
      region_count: region_count,
      labels: %{}
    }
  end

  @doc """
  Register a new store with PD.
  """
  def register_store(request, _stream) do
    Logger.info("RegisterStore", address: request.address)

    # Convert address to node name atom if provided, else use caller's node
    node_name =
      if request.address != "" do
        String.to_atom(request.address)
      else
        node()
      end

    case Server.register_store(node_name) do
      {:ok, registered_name, _leader} ->
        # Return a hash of the node name as store_id (proto wants uint64)
        store_id = :erlang.phash2(registered_name)
        %RegisterStoreResponse{store_id: store_id}

      {:ok, registered_name} ->
        store_id = :erlang.phash2(registered_name)
        %RegisterStoreResponse{store_id: store_id}

      {:error, reason} ->
        Logger.error("RegisterStore failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  @doc """
  Store heartbeat.
  """
  def heartbeat(request, _stream) do
    Logger.debug("Heartbeat", address: request.address)

    node_name =
      if request.address != "" do
        String.to_atom(request.address)
      else
        node()
      end

    # Process heartbeat
    heartbeat_result = Server.heartbeat(node_name)

    case heartbeat_result do
      result when result == :ok or is_tuple(result) ->
        # Get pending tasks from scheduler
        {tasks, _epoch} =
          try do
            PD.Scheduler.get_pending_tasks(request.address)
          catch
            _, _ -> {[], 0}
          end

        # Convert internal tasks to proto format
        proto_tasks = Enum.map(tasks, &operation_to_proto/1)
        %StoreHeartbeatResponse{tasks: proto_tasks}

      {:error, reason} ->
        Logger.warning("Heartbeat failed", address: request.address, reason: inspect(reason))
        %StoreHeartbeatResponse{tasks: []}
    end
  end

  defp operation_to_proto(%{type: :split_region} = op) do
    %Spiredb.Cluster.ScheduledTask{
      task_id: op[:task_id] || 0,
      leader_epoch: op[:leader_epoch] || 0,
      task:
        {:split,
         %Spiredb.Cluster.SplitRegion{
           region_id: op.region_id,
           split_key: op[:split_key] || <<>>,
           new_region_id: op[:new_region_id] || 0,
           new_peer_id: op[:new_peer_id] || 0
         }}
    }
  end

  defp operation_to_proto(%{type: :move_region} = op) do
    # Move region is implemented as transfer leader
    %Spiredb.Cluster.ScheduledTask{
      task_id: op[:task_id] || 0,
      leader_epoch: op[:leader_epoch] || 0,
      task:
        {:transfer_leader,
         %Spiredb.Cluster.TransferLeader{
           region_id: op.region_id,
           from_store_id: store_id_hash(op.from_store),
           to_store_id: store_id_hash(op.to_store)
         }}
    }
  end

  defp operation_to_proto(%{type: :add_replica} = op) do
    %Spiredb.Cluster.ScheduledTask{
      task_id: op[:task_id] || 0,
      leader_epoch: op[:leader_epoch] || 0,
      task:
        {:add_peer,
         %Spiredb.Cluster.AddPeer{
           region_id: op.region_id,
           store_id: store_id_hash(op.target_store),
           peer_id: op[:peer_id] || 0,
           is_learner: op[:is_learner] || false
         }}
    }
  end

  defp operation_to_proto(%{type: :remove_replica} = op) do
    %Spiredb.Cluster.ScheduledTask{
      task_id: op[:task_id] || 0,
      leader_epoch: op[:leader_epoch] || 0,
      task:
        {:remove_peer,
         %Spiredb.Cluster.RemovePeer{
           region_id: op.region_id,
           store_id: store_id_hash(op.target_store),
           peer_id: op[:peer_id] || 0
         }}
    }
  end

  defp operation_to_proto(_op), do: nil

  defp store_id_hash(store) when is_atom(store), do: :erlang.phash2(store)
  defp store_id_hash(store) when is_integer(store), do: store
  defp store_id_hash(_), do: 0

  # Private helpers

  defp region_to_proto(region) do
    # Convert atom store IDs to integer hashes for proto
    peers =
      Enum.map(region.stores || [], fn store_node ->
        store_id =
          if is_atom(store_node) do
            :erlang.phash2(store_node)
          else
            store_node
          end

        %Peer{store_id: store_id, role: :PEER_FOLLOWER}
      end)

    leader_id =
      if is_atom(region.leader) do
        :erlang.phash2(region.leader)
      else
        region.leader || 0
      end

    %Region{
      id: region.id,
      start_key: region.start_key || <<>>,
      end_key: region.end_key || <<>>,
      peers: peers,
      leader_store_id: leader_id,
      region_epoch: region.epoch || 0,
      state: :REGION_ACTIVE
    }
  end
end
