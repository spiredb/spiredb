defmodule PD.Schema.StatsCollectorTest do
  use ExUnit.Case, async: false
  alias PD.Schema.StatsCollector
  alias PD.Schema.Registry
  alias PD.Schema.Column

  setup do
    start_supervised!({Registry, name: PD.Schema.Registry})
    start_supervised!({StatsCollector, [name: PD.Schema.StatsCollector]})
    :ok
  end

  test "collects stats periodically" do
    columns = [%Column{name: "id", type: :int64}]
    Registry.create_table("stats_col_test", columns, ["id"])

    # Force immediate collection by sending message
    send(PD.Schema.StatsCollector, :collect_stats)

    # Since collection is async and currently just logs, we just verify it doesn't crash
    # In a real test we might verify mock interactions or side effects
    Process.sleep(100)
    assert Process.alive?(Process.whereis(PD.Schema.StatsCollector))
  end
end
