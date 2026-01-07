defmodule Store.KV.TTLTest do
  use ExUnit.Case, async: true

  alias Store.KV.TTL

  describe "encode_with_ttl/2" do
    test "encodes value with TTL" do
      encoded = TTL.encode_with_ttl("myvalue", 300)

      assert is_binary(encoded)
      # Should start with TTL marker
      assert <<0x01, _rest::binary>> = encoded
    end

    test "encodes without TTL when ttl is 0" do
      encoded = TTL.encode_with_ttl("myvalue", 0)

      assert <<0x00, "myvalue">> = encoded
    end
  end

  describe "encode_no_ttl/1" do
    test "encodes value without TTL" do
      encoded = TTL.encode_no_ttl("myvalue")

      assert <<0x00, "myvalue">> = encoded
    end
  end

  describe "decode/1" do
    test "decodes non-TTL value" do
      encoded = TTL.encode_no_ttl("myvalue")

      assert {:ok, "myvalue"} = TTL.decode(encoded)
    end

    test "decodes valid TTL value" do
      # Encode with 5 minute TTL
      encoded = TTL.encode_with_ttl("myvalue", 300)

      assert {:ok, "myvalue"} = TTL.decode(encoded)
    end

    test "returns expired for expired TTL value" do
      # Create value that expired in the past
      expiry_ts = System.system_time(:second) - 10
      encoded = <<0x01, expiry_ts::64-big, "myvalue">>

      assert {:expired, "myvalue"} = TTL.decode(encoded)
    end

    test "handles legacy non-encoded values" do
      assert {:ok, "legacy"} = TTL.decode("legacy")
    end
  end

  describe "ttl_remaining/1" do
    test "returns remaining seconds for TTL value" do
      encoded = TTL.encode_with_ttl("myvalue", 300)

      remaining = TTL.ttl_remaining(encoded)

      assert remaining > 0
      assert remaining <= 300
    end

    test "returns -1 for non-TTL value" do
      encoded = TTL.encode_no_ttl("myvalue")

      assert TTL.ttl_remaining(encoded) == -1
    end

    test "returns -2 for expired value" do
      expiry_ts = System.system_time(:second) - 10
      encoded = <<0x01, expiry_ts::64-big, "myvalue">>

      assert TTL.ttl_remaining(encoded) == -2
    end
  end

  describe "expired?/1" do
    test "returns false for non-TTL value" do
      encoded = TTL.encode_no_ttl("myvalue")

      refute TTL.expired?(encoded)
    end

    test "returns false for valid TTL value" do
      encoded = TTL.encode_with_ttl("myvalue", 300)

      refute TTL.expired?(encoded)
    end

    test "returns true for expired value" do
      expiry_ts = System.system_time(:second) - 10
      encoded = <<0x01, expiry_ts::64-big, "myvalue">>

      assert TTL.expired?(encoded)
    end
  end

  describe "expiry_timestamp/1" do
    test "returns timestamp for TTL value" do
      encoded = TTL.encode_with_ttl("myvalue", 300)

      ts = TTL.expiry_timestamp(encoded)

      assert is_integer(ts)
      assert ts > System.system_time(:second)
    end

    test "returns nil for non-TTL value" do
      encoded = TTL.encode_no_ttl("myvalue")

      assert TTL.expiry_timestamp(encoded) == nil
    end
  end

  describe "update_ttl/2" do
    test "updates TTL on existing TTL value" do
      original = TTL.encode_with_ttl("myvalue", 100)
      updated = TTL.update_ttl(original, 500)

      remaining = TTL.ttl_remaining(updated)

      assert remaining > 400
    end

    test "adds TTL to non-TTL value" do
      original = TTL.encode_no_ttl("myvalue")
      updated = TTL.update_ttl(original, 300)

      remaining = TTL.ttl_remaining(updated)

      assert remaining > 0
    end
  end

  describe "persist/1" do
    test "removes TTL from TTL value" do
      with_ttl = TTL.encode_with_ttl("myvalue", 300)
      persisted = TTL.persist(with_ttl)

      assert TTL.ttl_remaining(persisted) == -1
      assert {:ok, "myvalue"} = TTL.decode(persisted)
    end

    test "keeps non-TTL value unchanged" do
      no_ttl = TTL.encode_no_ttl("myvalue")
      persisted = TTL.persist(no_ttl)

      assert persisted == no_ttl
    end
  end
end
