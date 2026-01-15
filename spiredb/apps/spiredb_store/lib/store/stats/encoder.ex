defmodule Store.Stats.Encoder do
  @moduledoc """
  Encodes statistics values to JSON bytes for SpireSQL consumption.

  ## Encoding Format

  Uses JSON for type-safe encoding, consistent with filter pushdown:

  | Type | JSON Format |
  |------|-------------|
  | Int64 | `{"int": 42}` |
  | Float64 | `{"float": 3.14}` |
  | String | `{"str": "hello"}` |
  | Bytes | `{"bytes": "base64..."}` |
  """

  @doc """
  Encode a value to JSON bytes.
  """
  @spec encode(term()) :: binary()
  def encode(value) when is_integer(value) do
    Jason.encode!(%{int: value})
  end

  def encode(value) when is_float(value) do
    Jason.encode!(%{float: value})
  end

  def encode(value) when is_binary(value) do
    if String.valid?(value) do
      Jason.encode!(%{str: value})
    else
      Jason.encode!(%{bytes: Base.encode64(value)})
    end
  end

  def encode(true), do: Jason.encode!(%{bool: true})
  def encode(false), do: Jason.encode!(%{bool: false})
  def encode(nil), do: <<>>
  def encode(_value), do: <<>>
end
