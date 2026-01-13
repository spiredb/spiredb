defmodule Store.SlowQueryLog do
  @moduledoc """
  Slow query logging for command execution.

  Logs commands that take longer than the configured threshold.
  Uses telemetry for non-blocking emission.
  """

  require Logger

  @default_threshold_ms 100
  @telemetry_prefix [:spiredb, :slow_query]

  @doc """
  Get the slow query threshold in milliseconds.
  """
  def threshold_ms do
    Application.get_env(:spiredb_store, :slow_query_threshold_ms, @default_threshold_ms)
  end

  @doc """
  Log a slow query if it exceeds the threshold.

  ## Parameters
  - `command` - The command that was executed (e.g., ["GET", "key"])
  - `start_time` - Monotonic start time from `System.monotonic_time(:millisecond)`
  - `opts` - Optional metadata (e.g., [region_id: 1])
  """
  def maybe_log(command, start_time, opts \\ []) do
    duration_ms = System.monotonic_time(:millisecond) - start_time
    threshold = threshold_ms()

    if duration_ms >= threshold do
      log_slow_query(command, duration_ms, opts)
    end

    duration_ms
  end

  @doc """
  Wrap a function and log if it's slow.

  Returns the result of the function.
  """
  def measure(command, opts \\ [], fun) do
    start_time = System.monotonic_time(:millisecond)
    result = fun.()
    maybe_log(command, start_time, opts)
    result
  end

  @doc """
  Attach telemetry handler for slow query events.
  """
  def attach_handler do
    :telemetry.attach(
      "spiredb-slow-query-logger",
      @telemetry_prefix ++ [:logged],
      &handle_event/4,
      nil
    )
  end

  @doc """
  Get slow query telemetry event name.
  """
  def telemetry_event, do: @telemetry_prefix ++ [:logged]

  ## Private

  defp log_slow_query(command, duration_ms, opts) do
    cmd_str = format_command(command)
    region_id = Keyword.get(opts, :region_id)

    metadata = %{
      command: cmd_str,
      duration_ms: duration_ms,
      region_id: region_id
    }

    # Emit telemetry event
    :telemetry.execute(
      @telemetry_prefix ++ [:logged],
      %{duration_ms: duration_ms},
      %{command: cmd_str, region_id: region_id}
    )

    # Log warning
    Logger.warning("Slow query: #{cmd_str} took #{duration_ms}ms",
      duration_ms: duration_ms,
      region_id: region_id
    )

    metadata
  end

  defp format_command(command) when is_list(command) do
    command
    # Limit to first 3 elements to avoid huge logs
    |> Enum.take(3)
    |> Enum.map(&format_arg/1)
    |> Enum.join(" ")
  end

  defp format_command(command), do: inspect(command)

  defp format_arg(arg) when is_binary(arg) do
    if String.printable?(arg) and byte_size(arg) <= 50 do
      arg
    else
      "#{byte_size(arg)}B"
    end
  end

  defp format_arg(arg), do: inspect(arg)

  defp handle_event(_event, measurements, metadata, _config) do
    Logger.debug("Slow query telemetry: #{metadata.command} took #{measurements.duration_ms}ms")
  end
end
