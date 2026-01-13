defmodule Store.SlowQueryLogTest do
  use ExUnit.Case, async: true

  alias Store.SlowQueryLog

  describe "threshold_ms/0" do
    test "returns default threshold" do
      # Default is 100ms
      threshold = SlowQueryLog.threshold_ms()
      assert is_integer(threshold)
      assert threshold > 0
    end
  end

  describe "maybe_log/3" do
    test "returns duration" do
      start = System.monotonic_time(:millisecond)
      duration = SlowQueryLog.maybe_log(["GET", "key"], start)
      assert is_integer(duration)
      assert duration >= 0
    end
  end

  describe "measure/3" do
    test "executes function and returns result" do
      result = SlowQueryLog.measure(["GET", "key"], [], fn -> :test_result end)
      assert result == :test_result
    end

    test "measures function duration" do
      result =
        SlowQueryLog.measure(["SLEEP"], [], fn ->
          Process.sleep(10)
          :done
        end)

      assert result == :done
    end
  end

  describe "telemetry_event/0" do
    test "returns event name" do
      event = SlowQueryLog.telemetry_event()
      assert event == [:spiredb, :slow_query, :logged]
    end
  end

  describe "format_command" do
    test "handles list commands" do
      # Access private function via measure
      result = SlowQueryLog.measure(["GET", "mykey"], [], fn -> :ok end)
      assert result == :ok
    end

    test "handles binary values" do
      result = SlowQueryLog.measure(["SET", "key", <<1, 2, 3, 4, 5>>], [], fn -> :ok end)
      assert result == :ok
    end
  end
end
