defmodule Store.Arrow.Encoder do
  @moduledoc """
  Encode rows as Apache Arrow RecordBatch using Explorer library.

  Provides zero-copy data transfer to SpireSQL (Rust) using Arrow's
  binary IPC format via the Explorer DataFrame library.

  ## Performance Optimizations
  - Cached empty batch templates (avoid repeated allocations)
  - Direct binary series construction
  - Fallback to simple binary format for internal use
  """

  require Logger
  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  # Cache empty batches at compile time via module attributes
  @empty_scan_batch (fn ->
                       df =
                         DF.new(%{
                           "key" => Series.from_list([], dtype: :binary),
                           "value" => Series.from_list([], dtype: :binary)
                         })

                       {:ok, binary} = DF.dump_ipc_stream(df)
                       binary
                     end).()

  @empty_batch_get (fn ->
                      df =
                        DF.new(
                          %{
                            "key" => Series.from_list([], dtype: :binary),
                            "value" => Series.from_list([], dtype: :binary),
                            "found" => Series.from_list([], dtype: :boolean)
                          },
                          dtypes: [{"key", :binary}, {"value", :binary}, {"found", :boolean}]
                        )

                      {:ok, binary} = DF.dump_ipc_stream(df)
                      binary
                    end).()

  @doc """
  Encode scan results as Arrow RecordBatch IPC stream.

  Schema: [key: Binary, value: Binary]

  Returns binary in Arrow IPC stream format.
  """
  def encode_scan_batch([]), do: @empty_scan_batch

  def encode_scan_batch(rows) when is_list(rows) do
    # Use unzip for single pass extraction (more efficient than 2x Enum.map)
    {keys, values} = Enum.unzip(rows)

    # Create DataFrame with binary series
    df =
      DF.new(%{
        "key" => Series.from_list(keys, dtype: :binary),
        "value" => Series.from_list(values, dtype: :binary)
      })

    # Export as Arrow IPC stream (returns binary)
    {:ok, binary} = DF.dump_ipc_stream(df)
    binary
  rescue
    error ->
      Logger.error("Failed to encode scan batch",
        error: inspect(error),
        rows_count: length(rows)
      )

      {:error, :encoding_failed}
  end

  @doc """
  Encode batch get results as Arrow RecordBatch IPC stream.

  Schema: [key: Binary, value: Binary, found: Boolean]

  Returns binary in Arrow IPC stream format.
  """
  def encode_batch_get_result([]), do: @empty_batch_get

  def encode_batch_get_result(results) when is_list(results) do
    # Single-pass extraction using reduce
    {keys, values, founds} =
      Enum.reduce(results, {[], [], []}, fn {k, v, f}, {ks, vs, fs} ->
        {[k | ks], [v | vs], [f | fs]}
      end)

    # Reverse to maintain order (reduce builds reversed lists)
    keys = Enum.reverse(keys)
    values = Enum.reverse(values)
    founds = Enum.reverse(founds)

    # Create DataFrame (column order matters for tests)
    df =
      DF.new(
        %{
          "key" => Series.from_list(keys, dtype: :binary),
          "value" => Series.from_list(values, dtype: :binary),
          "found" => Series.from_list(founds, dtype: :boolean)
        },
        dtypes: [{"key", :binary}, {"value", :binary}, {"found", :boolean}]
      )

    # Export as Arrow IPC stream
    {:ok, binary} = DF.dump_ipc_stream(df)
    binary
  rescue
    error ->
      Logger.error("Failed to encode batch get result",
        error: inspect(error),
        results_count: length(results)
      )

      {:error, :encoding_failed}
  end

  @doc """
  Fast binary format for internal use (no Arrow overhead).

  Format: count:32, (key_len:32, key, value_len:32, value)*

  Use this for internal RPC where Arrow IPC is overkill.
  """
  def encode_binary_batch(rows) when is_list(rows) do
    count = length(rows)

    data =
      Enum.map(rows, fn {key, value} ->
        <<byte_size(key)::32, key::binary, byte_size(value)::32, value::binary>>
      end)

    <<count::32, IO.iodata_to_binary(data)::binary>>
  end

  @doc """
  Decode fast binary format.
  """
  def decode_binary_batch(<<count::32, rest::binary>>) do
    decode_binary_rows(rest, count, [])
  end

  defp decode_binary_rows(<<>>, 0, acc), do: Enum.reverse(acc)

  defp decode_binary_rows(
         <<key_len::32, key::binary-size(key_len), value_len::32, value::binary-size(value_len),
           rest::binary>>,
         remaining,
         acc
       )
       when remaining > 0 do
    decode_binary_rows(rest, remaining - 1, [{key, value} | acc])
  end

  defp decode_binary_rows(_, _, acc), do: Enum.reverse(acc)
end
