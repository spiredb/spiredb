defmodule Store.TaskExecutor do
  @moduledoc """
  Executes scheduled tasks from PD with leader epoch validation.

  Tasks are received via heartbeat responses. Each task contains:
  - task_id: unique identifier
  - leader_epoch: Raft term when task was issued

  Tasks are rejected if:
  - leader_epoch < current known epoch (stale task from old leader)
  - task_id already executed (duplicate)
  """

  use GenServer
  require Logger

  alias Spiredb.Cluster.{
    ScheduledTask,
    SplitRegion,
    MergeRegions,
    TransferLeader,
    AddPeer,
    RemovePeer,
    CompactRegion
  }

  defstruct [
    # Last known leader epoch
    known_epoch: 0,
    # Set of executed task IDs (for deduplication)
    executed_tasks: MapSet.new(),
    # Maximum executed tasks to track (prevent unbounded growth)
    max_tracked_tasks: 10_000,
    # Stats
    stats: %{
      executed: 0,
      rejected_stale: 0,
      rejected_duplicate: 0,
      failed: 0
    }
  ]

  ## Client API

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Execute a list of scheduled tasks.
  Returns list of {task_id, :ok | {:error, reason}}.
  """
  def execute_tasks(pid \\ __MODULE__, tasks) do
    GenServer.call(pid, {:execute_tasks, tasks}, 30_000)
  end

  @doc """
  Update the known leader epoch.
  Called when we learn of a new leader.
  """
  def update_epoch(pid \\ __MODULE__, epoch) do
    GenServer.cast(pid, {:update_epoch, epoch})
  end

  @doc """
  Get executor statistics.
  """
  def get_stats(pid \\ __MODULE__) do
    GenServer.call(pid, :get_stats)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    Logger.info("Store.TaskExecutor started")
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call({:execute_tasks, tasks}, _from, state) do
    {results, new_state} = execute_all(tasks, state)
    {:reply, results, new_state}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    {:reply, state.stats, state}
  end

  @impl true
  def handle_cast({:update_epoch, epoch}, state) do
    if epoch > state.known_epoch do
      Logger.info("TaskExecutor: Updated leader epoch", old: state.known_epoch, new: epoch)
      {:noreply, %{state | known_epoch: epoch}}
    else
      {:noreply, state}
    end
  end

  ## Private Functions

  defp execute_all(tasks, state) do
    Enum.map_reduce(tasks, state, fn task, acc ->
      {result, new_acc} = execute_one(task, acc)
      {{task.task_id, result}, new_acc}
    end)
  end

  defp execute_one(%ScheduledTask{} = task, state) do
    cond do
      # Check for stale task
      task.leader_epoch < state.known_epoch ->
        Logger.warning("Rejecting stale task",
          task_id: task.task_id,
          task_epoch: task.leader_epoch,
          known_epoch: state.known_epoch
        )

        new_stats = %{state.stats | rejected_stale: state.stats.rejected_stale + 1}
        {{:error, :stale_epoch}, %{state | stats: new_stats}}

      # Check for duplicate
      MapSet.member?(state.executed_tasks, task.task_id) ->
        Logger.debug("Skipping duplicate task", task_id: task.task_id)
        new_stats = %{state.stats | rejected_duplicate: state.stats.rejected_duplicate + 1}
        {{:error, :duplicate}, %{state | stats: new_stats}}

      # Execute the task
      true ->
        result = do_execute(task)

        # Update state
        new_executed =
          add_executed_task(state.executed_tasks, task.task_id, state.max_tracked_tasks)

        new_state =
          case result do
            :ok ->
              new_stats = %{state.stats | executed: state.stats.executed + 1}

              %{
                state
                | executed_tasks: new_executed,
                  stats: new_stats,
                  known_epoch: max(state.known_epoch, task.leader_epoch)
              }

            {:error, _} ->
              new_stats = %{state.stats | failed: state.stats.failed + 1}
              %{state | executed_tasks: new_executed, stats: new_stats}
          end

        {result, new_state}
    end
  end

  defp do_execute(%ScheduledTask{task: {:split, split}}) do
    execute_split(split)
  end

  defp do_execute(%ScheduledTask{task: {:merge, merge}}) do
    execute_merge(merge)
  end

  defp do_execute(%ScheduledTask{task: {:transfer_leader, transfer}}) do
    execute_transfer_leader(transfer)
  end

  defp do_execute(%ScheduledTask{task: {:add_peer, add}}) do
    execute_add_peer(add)
  end

  defp do_execute(%ScheduledTask{task: {:remove_peer, remove}}) do
    execute_remove_peer(remove)
  end

  defp do_execute(%ScheduledTask{task: {:compact, compact}}) do
    execute_compact(compact)
  end

  defp do_execute(_), do: {:error, :unknown_task}

  ## Task Handlers

  defp execute_split(%SplitRegion{} = split) do
    Logger.info("Executing SplitRegion",
      region_id: split.region_id,
      new_region_id: split.new_region_id,
      split_key: inspect(split.split_key)
    )

    # Region split requires:
    # 1. Update PD metadata to create new region
    # 2. Start new Raft group for new region
    # 3. Original region will stop serving keys >= split_key

    try do
      # Create the new region in PD
      case PD.Server.create_region(%{
             start_key: split.split_key,
             # Will inherit from original
             end_key: nil,
             stores: [node()]
           }) do
        {:ok, _region, _leader} ->
          # Start Raft server for new region
          nodes = [{split.new_region_id, node()}]

          case Store.Region.Raft.start_server(split.new_region_id, nodes) do
            :ok ->
              Logger.info("Region split complete",
                original: split.region_id,
                new: split.new_region_id
              )

              :ok

            {:error, reason} ->
              Logger.error("Failed to start Raft for new region: #{inspect(reason)}")
              {:error, {:raft_start_failed, reason}}
          end

        {:error, reason} ->
          Logger.error("Failed to create region in PD: #{inspect(reason)}")
          {:error, {:pd_create_failed, reason}}
      end
    catch
      kind, reason ->
        Logger.error("Split failed: #{inspect({kind, reason})}")
        {:error, :split_failed}
    end
  end

  defp execute_merge(%MergeRegions{} = merge) do
    Logger.info("Executing MergeRegions",
      source: merge.source_region_id,
      target: merge.target_region_id
    )

    # Region merge requires:
    # 1. Migrate data from source to target (for cross-node merges)
    # 2. Stop source region Raft
    # 3. Update PD metadata to expand target's key range

    try do
      # Step 1: Migrate data (noop for same-node, actual transfer for cross-node)
      case Store.Region.DataMigration.migrate_to_target(
             merge.source_region_id,
             merge.target_region_id
           ) do
        {:ok, migrated_count} ->
          Logger.info("Migrated #{migrated_count} keys",
            source: merge.source_region_id,
            target: merge.target_region_id
          )

          # Step 2: Stop the source region's Raft server
          source_server = Store.Region.Raft.server_id(merge.source_region_id)

          case :ra.stop_server(:default, source_server) do
            :ok ->
              Logger.info("Stopped source region Raft and completed merge",
                source: merge.source_region_id,
                target: merge.target_region_id
              )

              :ok

            {:error, reason} ->
              Logger.error("Failed to stop source region: #{inspect(reason)}")
              {:error, {:stop_failed, reason}}
          end

        {:error, reason} ->
          Logger.error("Data migration failed: #{inspect(reason)}")
          {:error, {:migration_failed, reason}}
      end
    catch
      kind, reason ->
        Logger.error("Merge failed: #{inspect({kind, reason})}")
        {:error, :merge_failed}
    end
  end

  defp execute_transfer_leader(%TransferLeader{} = transfer) do
    Logger.info("Executing TransferLeader",
      region: transfer.region_id,
      from: transfer.from_store_id,
      to: transfer.to_store_id
    )

    try do
      server_id = Store.Region.Raft.server_id(transfer.region_id)
      # Target server ID - need to construct from store_id
      # Note: store_id is a hash, we'd need a reverse lookup in production
      # For now, this demonstrates the API

      case :ra.transfer_leadership(server_id, server_id) do
        :ok ->
          Logger.info("Leadership transfer initiated", region: transfer.region_id)
          :ok

        {:error, reason} ->
          Logger.warning("Transfer leadership failed: #{inspect(reason)}")
          {:error, {:transfer_failed, reason}}

        {:timeout, _} ->
          Logger.warning("Transfer leadership timed out")
          {:error, :timeout}
      end
    catch
      kind, reason ->
        Logger.error("Transfer failed: #{inspect({kind, reason})}")
        {:error, :transfer_failed}
    end
  end

  defp execute_add_peer(%AddPeer{} = add) do
    Logger.info("Executing AddPeer",
      region: add.region_id,
      store: add.store_id,
      peer_id: add.peer_id,
      learner: add.is_learner
    )

    try do
      server_id = Store.Region.Raft.server_id(add.region_id)
      # Construct new member server ID
      # In production, we'd resolve store_id to node name
      new_member = {String.to_atom("region_#{add.region_id}"), node()}

      case :ra.add_member(server_id, new_member) do
        {:ok, _, _leader} ->
          Logger.info("Peer added successfully", region: add.region_id)
          :ok

        {:error, :already_member} ->
          Logger.info("Peer already a member", region: add.region_id)
          :ok

        {:error, reason} ->
          Logger.error("Add member failed: #{inspect(reason)}")
          {:error, {:add_member_failed, reason}}

        {:timeout, _} ->
          Logger.warning("Add member timed out")
          {:error, :timeout}
      end
    catch
      kind, reason ->
        Logger.error("Add peer failed: #{inspect({kind, reason})}")
        {:error, :add_peer_failed}
    end
  end

  defp execute_remove_peer(%RemovePeer{} = remove) do
    Logger.info("Executing RemovePeer",
      region: remove.region_id,
      store: remove.store_id,
      peer_id: remove.peer_id
    )

    try do
      server_id = Store.Region.Raft.server_id(remove.region_id)
      # Target member to remove
      member_to_remove = {String.to_atom("region_#{remove.region_id}"), node()}

      case :ra.remove_member(server_id, member_to_remove) do
        {:ok, _, _leader} ->
          Logger.info("Peer removed successfully", region: remove.region_id)
          :ok

        {:error, :not_member} ->
          Logger.info("Peer not a member", region: remove.region_id)
          :ok

        {:error, reason} ->
          Logger.error("Remove member failed: #{inspect(reason)}")
          {:error, {:remove_member_failed, reason}}

        {:timeout, _} ->
          Logger.warning("Remove member timed out")
          {:error, :timeout}
      end
    catch
      kind, reason ->
        Logger.error("Remove peer failed: #{inspect({kind, reason})}")
        {:error, :remove_peer_failed}
    end
  end

  defp execute_compact(%CompactRegion{} = compact) do
    Logger.info("Executing CompactRegion",
      region: compact.region_id,
      start_key: inspect(compact.start_key),
      end_key: inspect(compact.end_key)
    )

    try do
      # Get RocksDB reference from persistent_term (set by Store.KV.Engine)
      case :persistent_term.get(:spiredb_rocksdb_ref, nil) do
        nil ->
          Logger.error("RocksDB reference not found")
          {:error, :no_engine}

        db_ref ->
          # Compact the range (or full if no range specified)
          start_key = if compact.start_key == <<>>, do: :undefined, else: compact.start_key
          end_key = if compact.end_key == <<>>, do: :undefined, else: compact.end_key

          case :rocksdb.compact_range(db_ref, start_key, end_key, []) do
            :ok ->
              Logger.info("Compaction complete", region: compact.region_id)
              :ok

            {:error, reason} ->
              Logger.error("Compaction failed: #{inspect(reason)}")
              {:error, {:compact_failed, reason}}
          end
      end
    catch
      kind, reason ->
        Logger.error("Compact failed: #{inspect({kind, reason})}")
        {:error, :compact_failed}
    end
  end

  defp add_executed_task(set, task_id, max_size) do
    new_set = MapSet.put(set, task_id)

    if MapSet.size(new_set) > max_size do
      # Remove oldest entries (approximation - just clear half)
      new_set
      |> MapSet.to_list()
      |> Enum.take(div(max_size, 2))
      |> MapSet.new()
    else
      new_set
    end
  end
end
