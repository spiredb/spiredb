defmodule Store.KV.TTL do
  @moduledoc """
  TTL (Time-To-Live) management for keys.

  ## Architecture

  TTL is implemented using RocksDB's native TTL support combined with
  a metadata prefix encoding:

  1. **Value Encoding**: Values with TTL are prefixed with expiration timestamp
     Format: `<<0x01, expiry_ts::64-big, original_value::binary>>`
     Non-TTL values: `<<0x00, original_value::binary>>`

  2. **Background Cleanup**: A periodic GenServer scans for expired keys

  3. **Read-Time Filtering**: Expired keys are filtered on read

  ## Usage

      # Set with TTL (5 minutes)
      Store.KV.TTL.put_with_ttl(engine, "mykey", "value", 300)

      # Get (auto-filters expired)
      Store.KV.TTL.get(engine, "mykey")

      # Check TTL remaining
      Store.KV.TTL.ttl(engine, "mykey")
  """

  require Logger

  @ttl_marker 0x01
  @no_ttl_marker 0x00

  @type ttl_seconds :: non_neg_integer()

  @doc """
  Encode a value with TTL.
  """
  @spec encode_with_ttl(binary(), ttl_seconds()) :: binary()
  def encode_with_ttl(value, ttl_seconds) when ttl_seconds > 0 do
    expiry_ts = System.system_time(:second) + ttl_seconds
    <<@ttl_marker, expiry_ts::64-big, value::binary>>
  end

  def encode_with_ttl(value, _ttl_seconds) do
    # No TTL or TTL <= 0
    <<@no_ttl_marker, value::binary>>
  end

  @doc """
  Encode a value without TTL.
  """
  @spec encode_no_ttl(binary()) :: binary()
  def encode_no_ttl(value) do
    <<@no_ttl_marker, value::binary>>
  end

  @doc """
  Decode a value and check if expired.
  Returns `{:ok, value}` if valid, `{:expired, value}` if expired, or `{:ok, value}` for non-TTL.
  """
  @spec decode(binary()) :: {:ok, binary()} | {:expired, binary()} | {:error, :invalid_format}
  def decode(<<@ttl_marker, expiry_ts::64-big, value::binary>>) do
    now = System.system_time(:second)

    if now >= expiry_ts do
      {:expired, value}
    else
      {:ok, value}
    end
  end

  def decode(<<@no_ttl_marker, value::binary>>) do
    {:ok, value}
  end

  # Legacy values without marker (backwards compatibility)
  def decode(value) when is_binary(value) do
    {:ok, value}
  end

  def decode(_), do: {:error, :invalid_format}

  @doc """
  Get the remaining TTL in seconds for an encoded value.
  Returns -1 for non-TTL keys, -2 for expired keys.
  """
  @spec ttl_remaining(binary()) :: integer()
  def ttl_remaining(<<@ttl_marker, expiry_ts::64-big, _value::binary>>) do
    now = System.system_time(:second)
    remaining = expiry_ts - now

    if remaining > 0, do: remaining, else: -2
  end

  def ttl_remaining(<<@no_ttl_marker, _value::binary>>), do: -1
  def ttl_remaining(_), do: -1

  @doc """
  Check if an encoded value is expired.
  """
  @spec expired?(binary()) :: boolean()
  def expired?(<<@ttl_marker, expiry_ts::64-big, _value::binary>>) do
    System.system_time(:second) >= expiry_ts
  end

  def expired?(_), do: false

  @doc """
  Get the expiration timestamp for an encoded value.
  Returns nil for non-TTL values.
  """
  @spec expiry_timestamp(binary()) :: non_neg_integer() | nil
  def expiry_timestamp(<<@ttl_marker, expiry_ts::64-big, _value::binary>>), do: expiry_ts
  def expiry_timestamp(_), do: nil

  @doc """
  Update TTL on an existing encoded value.
  """
  @spec update_ttl(binary(), ttl_seconds()) :: binary()
  def update_ttl(<<@ttl_marker, _old_ts::64-big, value::binary>>, new_ttl) do
    encode_with_ttl(value, new_ttl)
  end

  def update_ttl(<<@no_ttl_marker, value::binary>>, new_ttl) do
    encode_with_ttl(value, new_ttl)
  end

  def update_ttl(value, new_ttl) when is_binary(value) do
    encode_with_ttl(value, new_ttl)
  end

  @doc """
  Remove TTL from an encoded value (make it persistent).
  """
  @spec persist(binary()) :: binary()
  def persist(<<@ttl_marker, _ts::64-big, value::binary>>) do
    encode_no_ttl(value)
  end

  def persist(<<@no_ttl_marker, _value::binary>> = encoded), do: encoded
  def persist(value) when is_binary(value), do: encode_no_ttl(value)
end
