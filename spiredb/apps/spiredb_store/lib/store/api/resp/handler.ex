defmodule Store.API.RESP.Handler do
  @moduledoc """
  Ranch protocol handler for RESP connections.

  Features:
  - Backpressure via pending command limits
  - Connection idle timeout
  - Command execution timeout
  """

  require Logger
  alias Store.API.RESP.Commands

  @behaviour :ranch_protocol

  # Configuration constants (can be made configurable later)
  @max_pending_commands 1000
  @command_timeout 30_000
  @idle_timeout 300_000

  # Rate limiting
  # 1 second
  @rate_limit_window 1_000
  @rate_limit_ops Application.compile_env(:spiredb_store, :resp_rate_limit, 100_000)

  def start_link(ref, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  def init(ref, transport, _opts) do
    {:ok, socket} = :ranch.handshake(ref)
    Logger.debug("RESP client connected")

    :ok = transport.setopts(socket, active: :once)

    state = %{
      socket: socket,
      transport: transport,
      buffer: <<>>,
      pending_count: 0,
      last_activity: System.monotonic_time(:millisecond),
      ops_this_window: 0,
      window_start: System.monotonic_time(:millisecond)
    }

    loop(state)
  end

  defp loop(state) do
    %{socket: socket, transport: transport, buffer: buffer} = state

    receive do
      {:tcp, ^socket, data} ->
        new_state = %{state | last_activity: System.monotonic_time(:millisecond)}
        new_buffer = buffer <> data

        case process_buffer(new_buffer, new_state) do
          {:ok, responses, remaining, updated_state} ->
            send_responses(transport, socket, responses)
            :ok = transport.setopts(socket, active: :once)
            loop(%{updated_state | buffer: remaining})

          {:continue, new_buffer, updated_state} ->
            :ok = transport.setopts(socket, active: :once)
            loop(%{updated_state | buffer: new_buffer})

          {:backpressure, _state} ->
            # Too many pending commands, send error and close
            transport.send(socket, "-ERR server busy, too many pending commands\r\n")
            Logger.warning("Connection closed due to backpressure")
            transport.close(socket)

          {:error, reason, _state} ->
            Logger.error("Parse error: #{inspect(reason)}")
            transport.send(socket, "-ERR protocol error\r\n")
            transport.close(socket)
        end

      {:tcp_closed, ^socket} ->
        Logger.debug("RESP client disconnected")
        :ok

      {:tcp_error, ^socket, reason} ->
        Logger.error("TCP error: #{inspect(reason)}")
        transport.close(socket)
    after
      @idle_timeout ->
        Logger.debug("RESP client idle timeout, closing connection")
        transport.close(socket)
    end
  end

  defp process_buffer(buffer, state), do: process_buffer(buffer, [], state)

  defp process_buffer(buffer, responses, state) do
    # Check backpressure
    if state.pending_count >= @max_pending_commands do
      {:backpressure, state}
    else
      case Redix.Protocol.parse(buffer) do
        {:ok, command, rest} ->
          case check_rate_limit(state) do
            {:rate_limited, state} ->
              response = {:error, "ERR rate limit exceeded"}
              process_buffer(rest, [response | responses], state)

            {:ok, state} ->
              response = execute_command_with_timeout(command)

              new_state = %{
                state
                | pending_count: state.pending_count + 1,
                  ops_this_window: state.ops_this_window + 1
              }

              process_buffer(rest, [response | responses], new_state)
          end

        {:continuation, _cont} ->
          if responses == [] do
            {:continue, buffer, state}
          else
            # Reset pending count after successful batch
            {:ok, Enum.reverse(responses), buffer, %{state | pending_count: 0}}
          end
      end
    end
  end

  defp check_rate_limit(state) do
    now = System.monotonic_time(:millisecond)

    if now - state.window_start >= @rate_limit_window do
      {:ok, %{state | ops_this_window: 0, window_start: now}}
    else
      if state.ops_this_window >= @rate_limit_ops do
        {:rate_limited, state}
      else
        {:ok, state}
      end
    end
  end

  # Simple commands that can be executed inline without Task spawn
  # These are fast enough that the Task overhead would be significant
  @inline_commands ~w(PING ECHO COMMAND INFO GET SET DEL EXISTS INCR DECR SETNX TTL PTTL TYPE DBSIZE)

  defp execute_command_with_timeout([cmd | args]) when is_binary(cmd) do
    normalized_cmd = String.upcase(cmd)
    command = [normalized_cmd | args]

    if normalized_cmd in @inline_commands do
      # Inline execution - no Task spawn for simple commands
      try do
        Commands.execute(command)
      catch
        kind, reason ->
          Logger.error("Inline command failed (#{kind}): #{inspect(reason)}")
          {:error, "ERR internal error"}
      end
    else
      # Async execution with timeout for complex commands
      execute_with_task(command, normalized_cmd)
    end
  end

  defp execute_command_with_timeout(_invalid), do: {:error, "ERR invalid command format"}

  defp execute_with_task(command, cmd_name) do
    task =
      Task.async(fn ->
        try do
          Commands.execute(command)
        catch
          :exit, reason ->
            Logger.error("Command execution exited: #{inspect(reason)}")
            {:error, "ERR internal error"}

          kind, reason ->
            Logger.error("Command execution failed (#{kind}): #{inspect(reason)}")
            {:error, "ERR internal error"}
        end
      end)

    case Task.yield(task, @command_timeout) || Task.shutdown(task) do
      {:ok, result} ->
        result

      nil ->
        Logger.warning("Command timed out: #{cmd_name}")
        {:error, "ERR command timeout"}
    end
  end

  defp send_responses(transport, socket, responses) do
    Enum.each(responses, fn response ->
      encoded = encode_response(response)
      transport.send(socket, encoded)
    end)
  end

  # Manually encode RESP responses
  defp encode_response(:ok), do: "+OK\r\n"
  defp encode_response(:pong), do: "+PONG\r\n"
  defp encode_response({:error, msg}) when is_binary(msg), do: "-#{msg}\r\n"
  defp encode_response({:error, atom}) when is_atom(atom), do: "-#{Atom.to_string(atom)}\r\n"
  defp encode_response(nil), do: "$-1\r\n"
  defp encode_response(int) when is_integer(int), do: ":#{int}\r\n"

  defp encode_response(str) when is_binary(str) do
    len = byte_size(str)
    "$#{len}\r\n#{str}\r\n"
  end

  defp encode_response(list) when is_list(list) do
    count = length(list)
    elements = Enum.map(list, &encode_response/1)
    ["*#{count}\r\n", elements]
  end

  defp encode_response(value),
    do: encode_response({:error, "ERR unsupported type: #{inspect(value)}"})
end
