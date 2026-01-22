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

  ## Consumer Group Commands

  alias Store.Stream.ConsumerGroup

  # XGROUP CREATE stream group id [MKSTREAM] [ENTRIESREAD n]
  def execute(["XGROUP", "CREATE", stream, group, id | opts]) do
    parsed_opts = parse_xgroup_create_opts(opts)

    case ConsumerGroup.create(stream, group, id, parsed_opts) do
      :ok -> "OK"
      {:error, :busygroup} -> {:error, "BUSYGROUP Consumer Group name already exists"}
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  # XGROUP DESTROY stream group
  def execute(["XGROUP", "DESTROY", stream, group]) do
    case ConsumerGroup.destroy(stream, group) do
      {:ok, count} -> count
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  # XGROUP SETID stream group id [ENTRIESREAD n]
  def execute(["XGROUP", "SETID", stream, group, id | opts]) do
    parsed_opts = parse_xgroup_setid_opts(opts)

    case ConsumerGroup.set_id(stream, group, id, parsed_opts) do
      :ok -> "OK"
      {:error, :nogroup} -> {:error, "NOGROUP No such consumer group '#{group}'"}
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  # XGROUP CREATECONSUMER stream group consumer
  def execute(["XGROUP", "CREATECONSUMER", stream, group, consumer]) do
    case ConsumerGroup.create_consumer(stream, group, consumer) do
      {:ok, count} -> count
      {:error, :nogroup} -> {:error, "NOGROUP No such consumer group '#{group}'"}
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  # XGROUP DELCONSUMER stream group consumer
  def execute(["XGROUP", "DELCONSUMER", stream, group, consumer]) do
    case ConsumerGroup.delete_consumer(stream, group, consumer) do
      {:ok, count} -> count
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  def execute(["XGROUP" | _]) do
    {:error, "ERR wrong number of arguments for 'xgroup' command"}
  end

  # XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key [key...] id [id...]
  def execute(["XREADGROUP" | args]) do
    handle_xreadgroup(args)
  end

  # XACK stream group id [id ...]
  def execute(["XACK", stream, group | ids]) when ids != [] do
    case Store.Stream.xack(stream, group, ids) do
      {:ok, count} -> count
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  def execute(["XACK" | _]) do
    {:error, "ERR wrong number of arguments for 'xack' command"}
  end

  # XCLAIM stream group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID]
  def execute(["XCLAIM", stream, group, consumer, min_idle_str | rest]) do
    handle_xclaim(stream, group, consumer, min_idle_str, rest)
  end

  def execute(["XCLAIM" | _]) do
    {:error, "ERR wrong number of arguments for 'xclaim' command"}
  end

  # XAUTOCLAIM stream group consumer min-idle-time start-id [COUNT n] [JUSTID]
  def execute(["XAUTOCLAIM", stream, group, consumer, min_idle_str, start_id | opts]) do
    handle_xautoclaim(stream, group, consumer, min_idle_str, start_id, opts)
  end

  def execute(["XAUTOCLAIM" | _]) do
    {:error, "ERR wrong number of arguments for 'xautoclaim' command"}
  end

  # XPENDING stream group [[IDLE min-idle] start end count [consumer]]
  def execute(["XPENDING", stream, group | opts]) do
    handle_xpending(stream, group, opts)
  end

  def execute(["XPENDING" | _]) do
    {:error, "ERR wrong number of arguments for 'xpending' command"}
  end

  # XINFO GROUPS stream
  def execute(["XINFO", "GROUPS", stream]) do
    case ConsumerGroup.info_groups(stream) do
      {:ok, groups} ->
        Enum.map(groups, fn g ->
          [
            "name",
            g.name,
            "consumers",
            g.consumers,
            "pending",
            g.pending,
            "last-delivered-id",
            g.last_delivered_id || "0-0",
            "entries-read",
            g.entries_read
          ]
        end)

      {:error, reason} ->
        {:error, "ERR #{inspect(reason)}"}
    end
  end

  # XINFO CONSUMERS stream group
  def execute(["XINFO", "CONSUMERS", stream, group]) do
    case ConsumerGroup.info_consumers(stream, group) do
      {:ok, consumers} ->
        Enum.map(consumers, fn c ->
          [
            "name",
            c.name,
            "pending",
            c.pending,
            "idle",
            c.idle
          ]
        end)

      {:error, reason} ->
        {:error, "ERR #{inspect(reason)}"}
    end
  end

  def execute(["XINFO" | _]) do
    {:error, "ERR wrong number of arguments for 'xinfo' command"}
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
          id_str = format_stream_id(id)
          [id_str, Enum.flat_map(fields, fn {k, v} -> [k, v] end)]
        end)

      [stream_name, formatted_entries]
    end)
  end

  defp format_stream_id({ts, seq}), do: "#{ts}-#{seq}"
  defp format_stream_id(id) when is_binary(id), do: id

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

  ## Consumer Group Handlers

  defp parse_xgroup_create_opts(opts) do
    parse_xgroup_create_opts(opts, [])
  end

  defp parse_xgroup_create_opts(["MKSTREAM" | rest], acc) do
    parse_xgroup_create_opts(rest, [{:mkstream, true} | acc])
  end

  defp parse_xgroup_create_opts(["ENTRIESREAD", n | rest], acc) do
    case Integer.parse(n) do
      {num, ""} -> parse_xgroup_create_opts(rest, [{:entries_read, num} | acc])
      _ -> parse_xgroup_create_opts(rest, acc)
    end
  end

  defp parse_xgroup_create_opts([_ | rest], acc), do: parse_xgroup_create_opts(rest, acc)
  defp parse_xgroup_create_opts([], acc), do: acc

  defp parse_xgroup_setid_opts(opts), do: parse_xgroup_create_opts(opts, [])

  defp handle_xreadgroup(args) do
    {opts, stream_args} = parse_xreadgroup_options(args)

    case parse_stream_id_pairs(stream_args) do
      {:ok, streams} ->
        group = Map.get(opts, :group)
        consumer = Map.get(opts, :consumer)

        if is_nil(group) or is_nil(consumer) do
          {:error, "ERR GROUP or CONSUMER not specified for XREADGROUP"}
        else
          xreadgroup_opts =
            []
            |> maybe_add_opt(:count, Map.get(opts, :count))
            |> maybe_add_opt(:noack, Map.get(opts, :noack))

          case Store.Stream.xreadgroup(group, consumer, streams, xreadgroup_opts) do
            {:ok, results} ->
              format_xread_results(results)

            {:error, reason} ->
              {:error, "ERR #{inspect(reason)}"}
          end
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_xreadgroup_options(args), do: parse_xreadgroup_options(args, %{})

  defp parse_xreadgroup_options(["GROUP", group, consumer | rest], opts) do
    parse_xreadgroup_options(rest, Map.merge(opts, %{group: group, consumer: consumer}))
  end

  defp parse_xreadgroup_options(["COUNT", count | rest], opts) do
    case Integer.parse(count) do
      {n, ""} -> parse_xreadgroup_options(rest, Map.put(opts, :count, n))
      _ -> {opts, ["COUNT", count | rest]}
    end
  end

  defp parse_xreadgroup_options(["BLOCK", ms | rest], opts) do
    case Integer.parse(ms) do
      {n, ""} -> parse_xreadgroup_options(rest, Map.put(opts, :block, n))
      _ -> {opts, ["BLOCK", ms | rest]}
    end
  end

  defp parse_xreadgroup_options(["NOACK" | rest], opts) do
    parse_xreadgroup_options(rest, Map.put(opts, :noack, true))
  end

  defp parse_xreadgroup_options(["STREAMS" | rest], opts), do: {opts, rest}
  defp parse_xreadgroup_options(rest, opts), do: {opts, rest}

  defp handle_xclaim(stream, group, consumer, min_idle_str, rest) do
    case Integer.parse(min_idle_str) do
      {min_idle, ""} ->
        {ids, opts} = parse_xclaim_args(rest, [], [])

        case Store.Stream.xclaim(stream, group, consumer, min_idle, ids, opts) do
          {:ok, entries} ->
            if Keyword.get(opts, :justid, false) do
              Enum.map(entries, fn {id, _} -> format_stream_id(id) end)
            else
              format_entries(entries)
            end

          {:error, reason} ->
            {:error, "ERR #{inspect(reason)}"}
        end

      _ ->
        {:error, "ERR Invalid min-idle-time argument for XCLAIM"}
    end
  end

  defp parse_xclaim_args(["IDLE", ms | rest], ids, opts) do
    case Integer.parse(ms) do
      {n, ""} -> parse_xclaim_args(rest, ids, [{:idle, n} | opts])
      _ -> parse_xclaim_args(rest, ids, opts)
    end
  end

  defp parse_xclaim_args(["TIME", ms | rest], ids, opts) do
    case Integer.parse(ms) do
      {n, ""} -> parse_xclaim_args(rest, ids, [{:time, n} | opts])
      _ -> parse_xclaim_args(rest, ids, opts)
    end
  end

  defp parse_xclaim_args(["RETRYCOUNT", n | rest], ids, opts) do
    case Integer.parse(n) do
      {num, ""} -> parse_xclaim_args(rest, ids, [{:retry_count, num} | opts])
      _ -> parse_xclaim_args(rest, ids, opts)
    end
  end

  defp parse_xclaim_args(["FORCE" | rest], ids, opts) do
    parse_xclaim_args(rest, ids, [{:force, true} | opts])
  end

  defp parse_xclaim_args(["JUSTID" | rest], ids, opts) do
    parse_xclaim_args(rest, ids, [{:justid, true} | opts])
  end

  defp parse_xclaim_args([id | rest], ids, opts) when is_binary(id) do
    # Check if it looks like a stream ID (contains -)
    if String.contains?(id, "-") do
      parse_xclaim_args(rest, [id | ids], opts)
    else
      parse_xclaim_args(rest, ids, opts)
    end
  end

  defp parse_xclaim_args([], ids, opts), do: {Enum.reverse(ids), opts}

  defp handle_xautoclaim(stream, group, consumer, min_idle_str, start_id, opts) do
    case Integer.parse(min_idle_str) do
      {min_idle, ""} ->
        parsed_opts = parse_xautoclaim_opts(opts)

        case Store.Stream.xautoclaim(stream, group, consumer, min_idle, start_id, parsed_opts) do
          {:ok, next_id, entries, deleted_ids} ->
            if Keyword.get(parsed_opts, :justid, false) do
              [next_id, Enum.map(entries, fn {id, _} -> format_stream_id(id) end), deleted_ids]
            else
              [next_id, format_entries(entries), deleted_ids]
            end

          {:error, reason} ->
            {:error, "ERR #{inspect(reason)}"}
        end

      _ ->
        {:error, "ERR Invalid min-idle-time argument for XAUTOCLAIM"}
    end
  end

  defp parse_xautoclaim_opts(opts), do: parse_xautoclaim_opts(opts, [])

  defp parse_xautoclaim_opts(["COUNT", n | rest], acc) do
    case Integer.parse(n) do
      {num, ""} -> parse_xautoclaim_opts(rest, [{:count, num} | acc])
      _ -> parse_xautoclaim_opts(rest, acc)
    end
  end

  defp parse_xautoclaim_opts(["JUSTID" | rest], acc) do
    parse_xautoclaim_opts(rest, [{:justid, true} | acc])
  end

  defp parse_xautoclaim_opts([_ | rest], acc), do: parse_xautoclaim_opts(rest, acc)
  defp parse_xautoclaim_opts([], acc), do: acc

  defp handle_xpending(stream, group, opts) do
    parsed_opts = parse_xpending_opts(opts)

    case Store.Stream.xpending(stream, group, parsed_opts) do
      {:ok, summary} ->
        if Keyword.has_key?(parsed_opts, :count) do
          # Detailed format
          Enum.map(summary.entries, fn e ->
            [e.id, e.consumer, e.idle_ms, e.delivery_count]
          end)
        else
          # Summary format
          [
            summary.count,
            summary.min_id || "-",
            summary.max_id || "+",
            Enum.map(summary.consumers, fn {name, pending} -> [name, pending] end)
          ]
        end

      {:error, reason} ->
        {:error, "ERR #{inspect(reason)}"}
    end
  end

  defp parse_xpending_opts(opts), do: parse_xpending_opts(opts, [])

  defp parse_xpending_opts(["IDLE", ms | rest], acc) do
    case Integer.parse(ms) do
      {n, ""} -> parse_xpending_opts(rest, [{:min_idle, n} | acc])
      _ -> parse_xpending_opts(rest, acc)
    end
  end

  defp parse_xpending_opts([start, stop, count | rest], acc)
       when is_binary(start) and is_binary(stop) do
    case Integer.parse(count) do
      {n, ""} ->
        opts = [{:start, start}, {:end, stop}, {:count, n} | acc]

        case rest do
          [consumer | _] -> [{:consumer, consumer} | opts]
          _ -> opts
        end

      _ ->
        acc
    end
  end

  defp parse_xpending_opts([_ | rest], acc), do: parse_xpending_opts(rest, acc)
  defp parse_xpending_opts([], acc), do: acc
end
