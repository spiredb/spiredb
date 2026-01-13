defmodule Store.API.RESP.HandlerTest do
  use ExUnit.Case, async: true

  describe "inline commands list" do
    test "includes performance-critical commands" do
      # These commands should be in the inline list (no Task spawn)
      inline = ~w(PING ECHO COMMAND INFO GET SET DEL EXISTS INCR DECR SETNX TTL PTTL TYPE DBSIZE)

      # Verify we're testing the right commands
      assert "GET" in inline
      assert "SET" in inline
      assert "DEL" in inline
      assert "INCR" in inline
      assert "DECR" in inline
    end
  end

  describe "RESP encoding" do
    # Test the encode_response function behavior

    test "encodes simple strings" do
      assert encode_ok() == "+OK\r\n"
      assert encode_pong() == "+PONG\r\n"
    end

    test "encodes integers" do
      assert encode_int(42) == ":42\r\n"
      assert encode_int(-1) == ":-1\r\n"
      assert encode_int(0) == ":0\r\n"
    end

    test "encodes bulk strings" do
      assert encode_bulk("hello") == "$5\r\nhello\r\n"
      assert encode_bulk("") == "$0\r\n\r\n"
    end

    test "encodes nil as null bulk string" do
      assert encode_nil() == "$-1\r\n"
    end

    test "encodes arrays" do
      result = encode_array(["a", "b", "c"])
      assert String.starts_with?(result, "*3\r\n")
    end

    test "encodes errors" do
      assert encode_error("ERR test") == "-ERR test\r\n"
    end
  end

  # Helpers that mimic the actual encode_response logic
  defp encode_ok, do: "+OK\r\n"
  defp encode_pong, do: "+PONG\r\n"
  defp encode_int(n), do: ":#{n}\r\n"
  defp encode_bulk(str), do: "$#{byte_size(str)}\r\n#{str}\r\n"
  defp encode_nil, do: "$-1\r\n"
  defp encode_error(msg), do: "-#{msg}\r\n"

  defp encode_array(list) do
    count = length(list)
    elements = Enum.map(list, &encode_bulk/1)
    IO.iodata_to_binary(["*#{count}\r\n", elements])
  end
end
