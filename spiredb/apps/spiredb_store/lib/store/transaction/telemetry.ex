defmodule Store.Transaction.Telemetry do
  @moduledoc """
  Transaction telemetry events for observability.

  Emits telemetry events for:
  - Transaction begin/commit/abort
  - Conflict detection
  - Lock wait times
  - Async commit phases
  """

  @doc """
  Emit transaction begin event.
  """
  def transaction_begin(txn) do
    :telemetry.execute(
      [:spiredb, :transaction, :begin],
      %{count: 1},
      %{
        txn_id: txn.id,
        isolation_level: txn.isolation
      }
    )
  end

  @doc """
  Emit transaction commit event.
  """
  def transaction_commit(txn, commit_ts, duration_us) do
    coordinator_type = get_coordinator_type(txn)

    :telemetry.execute(
      [:spiredb, :transaction, :commit],
      %{
        count: 1,
        duration_us: duration_us,
        key_count: map_size(txn.write_buffer)
      },
      %{
        txn_id: txn.id,
        isolation_level: txn.isolation,
        coordinator_type: coordinator_type,
        commit_ts: commit_ts
      }
    )
  end

  @doc """
  Emit transaction abort event.
  """
  def transaction_abort(txn, reason, duration_us) do
    :telemetry.execute(
      [:spiredb, :transaction, :abort],
      %{
        count: 1,
        duration_us: duration_us
      },
      %{
        txn_id: txn.id,
        isolation_level: txn.isolation,
        reason: reason
      }
    )
  end

  @doc """
  Emit conflict detection event.
  """
  def conflict_detected(txn, conflict_type, key) do
    :telemetry.execute(
      [:spiredb, :transaction, :conflict],
      %{count: 1},
      %{
        txn_id: txn.id,
        isolation_level: txn.isolation,
        conflict_type: conflict_type,
        key: key
      }
    )
  end

  @doc """
  Emit lock wait event.
  """
  def lock_wait(txn_id, key, wait_time_ms, result) do
    :telemetry.execute(
      [:spiredb, :transaction, :lock_wait],
      %{
        count: 1,
        wait_time_ms: wait_time_ms
      },
      %{
        txn_id: txn_id,
        key: key,
        result: result
      }
    )
  end

  @doc """
  Emit async commit event.
  """
  def async_commit(txn, phase, duration_us) do
    :telemetry.execute(
      [:spiredb, :transaction, :async_commit],
      %{
        count: 1,
        duration_us: duration_us
      },
      %{
        txn_id: txn.id,
        phase: phase
      }
    )
  end

  @doc """
  Emit deadlock detection event.
  """
  def deadlock_detected(txn_id, holding_txn_id) do
    :telemetry.execute(
      [:spiredb, :transaction, :deadlock],
      %{count: 1},
      %{
        txn_id: txn_id,
        holding_txn_id: holding_txn_id
      }
    )
  end

  ## Private

  defp get_coordinator_type(txn) do
    key_count = map_size(txn.write_buffer)

    cond do
      key_count == 0 -> :empty
      key_count == 1 -> :single_key
      key_count > 1 -> :async_commit
      true -> :unknown
    end
  end
end
