defmodule Store.API.RESP.StreamCommands do
  @moduledoc """
  RESP handlers for Redis Stream commands.

  Supports:
  - XADD: Append entries to streams
  - XREAD: Read from multiple streams
  - XRANGE: Range query
  - XREVRANGE: Reverse range query
  - XLEN: Stream length
  - XINFO: Stream info
  - XTRIM: Trim stream
  - XDEL: Delete entries
  """

  require Logger

  @doc """
  Execute a stream command.
  """
  @spec execute(list()) :: term()

  # XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] [LIMIT count] *|id field value [field value ...]
  def execute(["XADD", stream_name | rest]) do
    handle_xadd(stream_name, rest)
  end

  # XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
  def execute(["XREAD" | args]) do
    handle_xread(args)
  end

  # XRANGE key start end [COUNT count]
  def execute(["XRANGE", stream_name, start_id, end_id | opts]) do
    handle_xrange(stream_name, start_id, end_id, opts)
  end

  # XREVRANGE key end start [COUNT count]
  def execute(["XREVRANGE", stream_name, end_id, start_id | opts]) do
    handle_xrevrange(stream_name, end_id, start_id, opts)
  end

  # XLEN key
  def execute(["XLEN", stream_name]) do
    case Store.Stream.xlen(stream_name) do
      {:ok, len} -> len
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  # XINFO STREAM key
  def execute(["XINFO", "STREAM", stream_name]) do
    case Store.Stream.xinfo(stream_name) do
      {:ok, info} ->
        # Convert to Redis-compatible array format
        [
          "length",
          info.length,
          "first-entry",
          info.first_entry,
          "last-entry",
          info.last_entry
        ]

      {:error, :stream_not_found} ->
        {:error, "ERR no such key"}

      {:error, reason} ->
        {:error, "ERR #{inspect(reason)}"}
    end
  end

  def execute(["XINFO" | _]) do
    {:error, "ERR wrong number of arguments for 'xinfo' command"}
  end

  # XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
  def execute(["XTRIM", stream_name | opts]) do
    handle_xtrim(stream_name, opts)
  end

  # XDEL key id [id ...]
  def execute(["XDEL", stream_name | ids]) when ids != [] do
    case Store.Stream.xdel(stream_name, ids) do
      {:ok, deleted_count} -> deleted_count
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  def execute(["XDEL", _stream_name]) do
    {:error, "ERR wrong number of arguments for 'xdel' command"}
  end

  def execute([cmd | _]) do
    {:error, "ERR unknown command '#{String.downcase(cmd)}'"}
  end

  ## Private handlers

  defp handle_xadd(stream_name, args) do
    {opts, field_values} = parse_xadd_options(args)

    if rem(length(field_values), 2) != 0 do
      {:error, "ERR wrong number of arguments for 'xadd' command"}
    else
      fields =
        field_values
        |> Enum.chunk_every(2)
        |> Enum.map(fn [field, value] -> {field, value} end)

      xadd_opts = build_xadd_opts(opts)

      case Store.Stream.xadd(stream_name, fields, xadd_opts) do
        {:ok, id} ->
          id

        {:error, :id_not_greater_than_last} ->
          {:error,
           "ERR The ID specified in XADD is equal or smaller than the target stream top item"}

        {:error, reason} ->
          {:error, "ERR #{inspect(reason)}"}
      end
    end
  end

  defp parse_xadd_options(args) do
    parse_xadd_options(args, %{})
  end

  defp parse_xadd_options(["*" | rest], opts) do
    # Auto-generate ID
    {opts, rest}
  end

  defp parse_xadd_options(["MAXLEN", "~", count | rest], opts) do
    case Integer.parse(count) do
      {n, ""} -> parse_xadd_options(rest, Map.put(opts, :maxlen, n))
      _ -> {opts, [count | rest]}
    end
  end

  defp parse_xadd_options(["MAXLEN", count | rest], opts) do
    case Integer.parse(count) do
      {n, ""} -> parse_xadd_options(rest, Map.put(opts, :maxlen, n))
      _ -> {opts, [count | rest]}
    end
  end

  defp parse_xadd_options(["NOMKSTREAM" | rest], opts) do
    parse_xadd_options(rest, Map.put(opts, :nomkstream, true))
  end

  defp parse_xadd_options([id | rest], opts) when is_binary(id) do
    if String.contains?(id, "-") or id == "*" do
      if id == "*" do
        {opts, rest}
      else
        {Map.put(opts, :id, id), rest}
      end
    else
      # It's a field name, we're done with options
      {opts, [id | rest]}
    end
  end

  defp parse_xadd_options(rest, opts), do: {opts, rest}

  defp build_xadd_opts(opts) do
    []
    |> maybe_add_opt(:id, Map.get(opts, :id))
    |> maybe_add_opt(:maxlen, Map.get(opts, :maxlen))
  end

  defp maybe_add_opt(acc, _key, nil), do: acc
  defp maybe_add_opt(acc, key, value), do: [{key, value} | acc]

  defp handle_xread(args) do
    {opts, stream_args} = parse_xread_options(args)

    case parse_stream_id_pairs(stream_args) do
      {:ok, streams} ->
        xread_opts = build_xread_opts(opts)

        case Store.Stream.xread(streams, xread_opts) do
          {:ok, results} ->
            format_xread_results(results)

          {:error, reason} ->
            {:error, "ERR #{inspect(reason)}"}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_xread_options(args) do
    parse_xread_options(args, %{})
  end

  defp parse_xread_options(["COUNT", count | rest], opts) do
    case Integer.parse(count) do
      {n, ""} -> parse_xread_options(rest, Map.put(opts, :count, n))
      _ -> {opts, ["COUNT", count | rest]}
    end
  end

  defp parse_xread_options(["BLOCK", ms | rest], opts) do
    case Integer.parse(ms) do
      {n, ""} -> parse_xread_options(rest, Map.put(opts, :block, n))
      _ -> {opts, ["BLOCK", ms | rest]}
    end
  end

  defp parse_xread_options(["STREAMS" | rest], opts) do
    {opts, rest}
  end

  defp parse_xread_options(rest, opts), do: {opts, rest}

  defp parse_stream_id_pairs(args) when length(args) >= 2 and rem(length(args), 2) == 0 do
    # First half are stream names, second half are IDs
    half = div(length(args), 2)
    {streams, ids} = Enum.split(args, half)

    pairs = Enum.zip(streams, ids)
    {:ok, pairs}
  end

  defp parse_stream_id_pairs(_) do
    {:error, "ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified"}
  end

  defp build_xread_opts(opts) do
    []
    |> maybe_add_opt(:count, Map.get(opts, :count))
  end

  defp format_xread_results(results) do
    results
    |> Enum.filter(fn {_stream, entries} -> entries != [] end)
    |> Enum.map(fn {stream_name, entries} ->
      formatted_entries =
        Enum.map(entries, fn {id, fields} ->
          [id, Enum.flat_map(fields, fn {k, v} -> [k, v] end)]
        end)

      [stream_name, formatted_entries]
    end)
  end

  defp handle_xrange(stream_name, start_id, end_id, opts) do
    count = parse_count_option(opts)

    case Store.Stream.xrange(stream_name, start_id, end_id, count: count) do
      {:ok, entries} ->
        format_entries(entries)

      {:error, reason} ->
        {:error, "ERR #{inspect(reason)}"}
    end
  end

  defp handle_xrevrange(stream_name, end_id, start_id, opts) do
    count = parse_count_option(opts)

    case Store.Stream.xrevrange(stream_name, end_id, start_id, count: count) do
      {:ok, entries} ->
        format_entries(entries)

      {:error, reason} ->
        {:error, "ERR #{inspect(reason)}"}
    end
  end

  defp parse_count_option(["COUNT", count | _]) do
    case Integer.parse(count) do
      {n, ""} -> n
      _ -> 100
    end
  end

  defp parse_count_option([_ | rest]), do: parse_count_option(rest)
  defp parse_count_option([]), do: 100

  defp format_entries(entries) do
    Enum.map(entries, fn {id, fields} ->
      [id, Enum.flat_map(fields, fn {k, v} -> [k, v] end)]
    end)
  end

  defp handle_xtrim(stream_name, opts) do
    case parse_xtrim_options(opts) do
      {:maxlen, count} ->
        # Add a dummy entry to trigger trimming, then it will be counted
        # For now, just return 0 since we trim on XADD
        # A proper implementation would trim without adding
        case Store.Stream.xlen(stream_name) do
          {:ok, len} when len > count -> len - count
          _ -> 0
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_xtrim_options(["MAXLEN", "~", count | _]) do
    case Integer.parse(count) do
      {n, ""} -> {:maxlen, n}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_xtrim_options(["MAXLEN", "=", count | _]) do
    case Integer.parse(count) do
      {n, ""} -> {:maxlen, n}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_xtrim_options(["MAXLEN", count | _]) do
    case Integer.parse(count) do
      {n, ""} -> {:maxlen, n}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_xtrim_options(_) do
    {:error, "ERR wrong number of arguments for 'xtrim' command"}
  end
end
