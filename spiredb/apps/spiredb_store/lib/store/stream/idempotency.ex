defmodule Store.Stream.Idempotency do
  @moduledoc """
  Track processed idempotency keys to prevent duplicate writes.

  Stored in RocksDB meta CF.
  Key format: "idem:{idempotency_key}"
  Value: {expiration_ts, result}
  """

  # 24 hours
  @default_ttl 86_400
  @prefix "idem:"
  @meta_cf "meta"

  def check_and_store(store_ref, idempotency_key, result, opts \\ []) do
    key = encode_key(idempotency_key)
    ttl = Keyword.get(opts, :ttl, @default_ttl)
    now = System.system_time(:second)
    expiration = now + ttl

    case get_meta(store_ref, key) do
      {:ok, binary} ->
        {exp, stored_result} = :erlang.binary_to_term(binary)

        if now > exp do
          # Expired, overwrite
          # Note: this is a race if concurrent, but TTL expiry race is acceptable
          write_new(store_ref, key, result, expiration)
          :ok
        else
          {:error, :already_processed, stored_result}
        end

      :not_found ->
        write_new(store_ref, key, result, expiration)
        :ok

      error ->
        error
    end
  end

  def is_processed?(store_ref, idempotency_key) do
    key = encode_key(idempotency_key)
    now = System.system_time(:second)

    case get_meta(store_ref, key) do
      {:ok, binary} ->
        {exp, _result} = :erlang.binary_to_term(binary)
        now <= exp

      _ ->
        false
    end
  end

  ## Private

  defp encode_key(key), do: @prefix <> key

  defp write_new(store_ref, key, result, expiration) do
    value = :erlang.term_to_binary({expiration, result})
    put_meta(store_ref, key, value)
  end

  defp get_meta_cf(%{db: db, cfs: cfs}) do
    case Map.get(cfs, @meta_cf) do
      nil -> nil
      cf -> {db, cf}
    end
  end

  defp get_meta_cf(_), do: nil

  defp put_meta(store_ref, key, value) do
    case get_meta_cf(store_ref) do
      {db, cf} ->
        :rocksdb.put(db, cf, key, value, [])

      nil ->
        {:error, :meta_cf_not_found}
    end
  end

  defp get_meta(store_ref, key) do
    case get_meta_cf(store_ref) do
      {db, cf} ->
        case :rocksdb.get(db, cf, key, []) do
          {:ok, val} -> {:ok, val}
          :not_found -> :not_found
          {:error, reason} -> {:error, reason}
        end

      nil ->
        {:error, :meta_cf_not_found}
    end
  end
end
