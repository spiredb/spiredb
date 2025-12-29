defmodule PD.API.GRPC do
  @moduledoc """
  gRPC service for Placement Driver metadata queries.

  Provides region discovery and routing information for SpireSQL
  to enable distributed scan operations.
  """

  use GRPC.Server, service: SpireDb.Spiredb.Pd.PlacementDriver.Service

  require Logger
  alias PD.Server

  @doc """
  Get all regions for a table.

  For now, returns all regions since we don't have table-level partitioning yet.
  """
  def get_table_regions(request, _stream) do
    Logger.debug("GetTableRegions", table: request.table_name)

    case Server.get_all_regions() do
      {:ok, regions} ->
        response = %{
          regions: Enum.map(regions, &region_to_proto/1)
        }

        {:ok, response}

      {:error, reason} ->
        Logger.error("GetTableRegions failed", reason: inspect(reason))

        {:error,
         %GRPC.RPCError{status: :internal, message: "Failed to get regions: #{inspect(reason)}"}}
    end
  end

  @doc """
  Get region metadata for a specific region ID.
  """
  def get_region(request, _stream) do
    Logger.debug("GetRegion", region_id: request.region_id)

    case Server.get_region_by_id(request.region_id) do
      {:ok, region} ->
        {:ok, region_to_proto(region)}

      {:error, :not_found} ->
        {:error, %GRPC.RPCError{status: :not_found, message: "Region not found"}}

      {:error, reason} ->
        Logger.error("GetRegion failed", region_id: request.region_id, reason: inspect(reason))

        {:error,
         %GRPC.RPCError{status: :internal, message: "Failed to get region: #{inspect(reason)}"}}
    end
  end

  @doc """
  Register a store node with PD.

  Existing functionality, now exposed via gRPC.
  """
  def register_store(request, _stream) do
    Logger.info("RegisterStore", node: request.node_name)

    case Server.register_store(request.node_name) do
      {:ok, _} ->
        {:ok, %{success: true}}

      {:error, reason} ->
        Logger.error("RegisterStore failed", node: request.node_name, reason: inspect(reason))
        {:ok, %{success: false}}
    end
  end

  @doc """
  Heartbeat from a store node.

  Existing functionality, now exposed via gRPC.
  """
  def heartbeat(request, _stream) do
    Logger.debug("Heartbeat", node: request.node_name)

    case Server.heartbeat(request.node_name) do
      :ok ->
        {:ok, %{success: true}}

      {:error, reason} ->
        Logger.warn("Heartbeat failed", node: request.node_name, reason: inspect(reason))
        {:ok, %{success: false}}
    end
  end

  # Private helpers

  defp region_to_proto(region) do
    %{
      region_id: region.id,
      start_key: region.start_key || "",
      end_key: region.end_key || "",
      leader_node: region.leader || "",
      followers: region.stores || [],
      state: :REGION_STATE_ACTIVE
    }
  end
end
