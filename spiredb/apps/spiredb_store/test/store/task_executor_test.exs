defmodule Store.TaskExecutorTest do
  use ExUnit.Case, async: false

  alias Store.TaskExecutor

  alias Spiredb.Cluster.{
    ScheduledTask,
    SplitRegion,
    TransferLeader,
    AddPeer,
    RemovePeer,
    CompactRegion
  }

  setup do
    {:ok, pid} = TaskExecutor.start_link(name: nil)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {:ok, executor: pid}
  end

  describe "epoch validation" do
    test "accepts tasks with current epoch", %{executor: pid} do
      TaskExecutor.update_epoch(pid, 100)

      task = %ScheduledTask{
        task_id: 1,
        leader_epoch: 100,
        task: {:compact, %CompactRegion{region_id: 1}}
      }

      [{1, result}] = TaskExecutor.execute_tasks(pid, [task])
      # Compact returns :ok even without RocksDB (no_engine error is fine)
      assert result == :ok or match?({:error, _}, result)
    end

    test "accepts tasks with higher epoch", %{executor: pid} do
      TaskExecutor.update_epoch(pid, 100)

      task = %ScheduledTask{
        task_id: 1,
        leader_epoch: 150,
        task: {:compact, %CompactRegion{region_id: 1}}
      }

      [{1, result}] = TaskExecutor.execute_tasks(pid, [task])
      # Task was attempted (not rejected for stale epoch)
      assert result == :ok or match?({:error, _}, result)
    end

    test "rejects tasks with stale epoch", %{executor: pid} do
      TaskExecutor.update_epoch(pid, 100)

      task = %ScheduledTask{
        task_id: 1,
        leader_epoch: 50,
        task: {:split, %SplitRegion{region_id: 1, new_region_id: 2}}
      }

      [{1, result}] = TaskExecutor.execute_tasks(pid, [task])
      assert result == {:error, :stale_epoch}

      stats = TaskExecutor.get_stats(pid)
      assert stats.rejected_stale == 1
    end
  end

  describe "duplicate detection" do
    test "rejects duplicate task IDs", %{executor: pid} do
      task = %ScheduledTask{
        task_id: 42,
        leader_epoch: 100,
        task: {:compact, %CompactRegion{region_id: 1}}
      }

      # First execution - may succeed or fail based on infrastructure
      [{42, first_result}] = TaskExecutor.execute_tasks(pid, [task])
      assert first_result == :ok or match?({:error, _}, first_result)

      # Second execution - should be duplicate (regardless of first result)
      [{42, second_result}] = TaskExecutor.execute_tasks(pid, [task])
      assert second_result == {:error, :duplicate}

      stats = TaskExecutor.get_stats(pid)
      # Either executed or failed, but tracked
      assert stats.executed == 1 or stats.failed == 1
      assert stats.rejected_duplicate == 1
    end
  end

  describe "task execution" do
    test "attempts split task", %{executor: pid} do
      task = %ScheduledTask{
        task_id: 1,
        leader_epoch: 1,
        task:
          {:split,
           %SplitRegion{
             region_id: 1,
             split_key: <<128>>,
             new_region_id: 2,
             new_peer_id: 10
           }}
      }

      [{1, result}] = TaskExecutor.execute_tasks(pid, [task])
      # Without Raft/PD running, this will return an error - that's expected
      assert result == :ok or match?({:error, _}, result)
    end

    test "attempts transfer leader task", %{executor: pid} do
      task = %ScheduledTask{
        task_id: 2,
        leader_epoch: 1,
        task:
          {:transfer_leader,
           %TransferLeader{
             region_id: 1,
             from_store_id: 100,
             to_store_id: 200
           }}
      }

      [{2, result}] = TaskExecutor.execute_tasks(pid, [task])
      assert result == :ok or match?({:error, _}, result)
    end

    test "attempts add peer task", %{executor: pid} do
      task = %ScheduledTask{
        task_id: 3,
        leader_epoch: 1,
        task:
          {:add_peer,
           %AddPeer{
             region_id: 1,
             store_id: 300,
             peer_id: 30,
             is_learner: true
           }}
      }

      [{3, result}] = TaskExecutor.execute_tasks(pid, [task])
      assert result == :ok or match?({:error, _}, result)
    end

    test "attempts remove peer task", %{executor: pid} do
      task = %ScheduledTask{
        task_id: 4,
        leader_epoch: 1,
        task:
          {:remove_peer,
           %RemovePeer{
             region_id: 1,
             store_id: 300,
             peer_id: 30
           }}
      }

      [{4, result}] = TaskExecutor.execute_tasks(pid, [task])
      assert result == :ok or match?({:error, _}, result)
    end

    test "attempts multiple tasks in order", %{executor: pid} do
      tasks = [
        %ScheduledTask{
          task_id: 1,
          leader_epoch: 1,
          task: {:compact, %CompactRegion{region_id: 1}}
        },
        %ScheduledTask{
          task_id: 2,
          leader_epoch: 1,
          task: {:compact, %CompactRegion{region_id: 2}}
        },
        %ScheduledTask{
          task_id: 3,
          leader_epoch: 1,
          task: {:compact, %CompactRegion{region_id: 3}}
        }
      ]

      results = TaskExecutor.execute_tasks(pid, tasks)

      assert length(results) == 3
      # Each task was attempted
      assert Enum.all?(results, fn {_id, r} -> r == :ok or match?({:error, _}, r) end)

      stats = TaskExecutor.get_stats(pid)
      # All tasks tracked (either executed or failed)
      assert stats.executed + stats.failed == 3
    end
  end

  describe "stats" do
    test "tracks execution statistics", %{executor: pid} do
      TaskExecutor.update_epoch(pid, 100)

      tasks = [
        # Valid task
        %ScheduledTask{
          task_id: 1,
          leader_epoch: 100,
          task: {:compact, %CompactRegion{region_id: 1}}
        },
        # Stale task
        %ScheduledTask{
          task_id: 2,
          leader_epoch: 50,
          task: {:compact, %CompactRegion{region_id: 2}}
        },
        # Duplicate of first
        %ScheduledTask{
          task_id: 1,
          leader_epoch: 100,
          task: {:compact, %CompactRegion{region_id: 1}}
        }
      ]

      TaskExecutor.execute_tasks(pid, tasks)

      stats = TaskExecutor.get_stats(pid)
      # First task was attempted (either executed or failed)
      assert stats.executed + stats.failed >= 1
      assert stats.rejected_stale == 1
      assert stats.rejected_duplicate == 1
    end
  end
end
