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
    Store,
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

    case Server.get_region(request.region_id) do
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

    case Server.get_all_regions() do
      {:ok, regions} ->
        %RegionList{regions: Enum.map(regions, &region_to_proto/1)}

      {:error, reason} ->
        Logger.error("GetTableRegions failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  @doc """
  Get store by ID.
  """
  def get_store(request, _stream) do
    Logger.debug("GetStore", store_id: request.store_id)

    case Server.get_store(request.store_id) do
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
    case Server.list_stores() do
      {:ok, stores} ->
        %StoreList{stores: Enum.map(stores, &store_to_proto/1)}

      {:error, reason} ->
        Logger.error("ListStores failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  @doc """
  Register a new store with PD.
  """
  def register_store(request, _stream) do
    Logger.info("RegisterStore", address: request.address)

    case Server.register_store(request.address, request.capacity, request.labels) do
      {:ok, store_id} ->
        %RegisterStoreResponse{store_id: store_id}

      {:ok, store_id, _leader} ->
        %RegisterStoreResponse{store_id: store_id}

      {:error, reason} ->
        Logger.error("RegisterStore failed", reason: inspect(reason))
        raise GRPC.RPCError, status: :internal, message: "Failed: #{inspect(reason)}"
    end
  end

  @doc """
  Store heartbeat.
  """
  def report_store_heartbeat(request, _stream) do
    Logger.debug("StoreHeartbeat", store_id: request.store_id)

    case Server.heartbeat(request.store_id, request) do
      :ok ->
        %StoreHeartbeatResponse{}

      {:ok, :ok, _leader} ->
        %StoreHeartbeatResponse{}

      {:error, reason} ->
        Logger.warning("Heartbeat failed", store_id: request.store_id, reason: inspect(reason))
        %StoreHeartbeatResponse{}
    end
  end

  # Private helpers

  defp region_to_proto(region) do
    %Region{
      id: region.id,
      start_key: region.start_key || <<>>,
      end_key: region.end_key || <<>>,
      peers:
        Enum.map(region.stores || [], fn store_id ->
          %Peer{store_id: store_id, role: :PEER_FOLLOWER}
        end),
      leader_store_id: region.leader || 0,
      region_epoch: region.epoch || 0,
      state: :REGION_ACTIVE
    }
  end

  defp store_to_proto(store) do
    %Store{
      id: store.id,
      address: store.address || "",
      state: :STORE_UP,
      capacity: store.capacity || 0,
      available: store.available || 0,
      region_count: store.region_count || 0,
      labels: store.labels || %{}
    }
  end
end
