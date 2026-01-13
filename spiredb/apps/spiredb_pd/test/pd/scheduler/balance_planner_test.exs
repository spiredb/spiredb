defmodule PD.Scheduler.BalancePlannerTest do
  use ExUnit.Case, async: true

  alias PD.Scheduler.BalancePlanner

  describe "compute_plan/2" do
    test "detects balanced cluster" do
      metrics = %{
        stores: [
          %{node: "store-1", region_count: 8, regions: [1, 2, 3, 4, 5, 6, 7, 8], is_alive: true},
          %{
            node: "store-2",
            region_count: 8,
            regions: [9, 10, 11, 12, 13, 14, 15, 16],
            is_alive: true
          }
        ],
        total_regions: 16,
        timestamp: DateTime.utc_now()
      }

      plan = BalancePlanner.compute_plan(metrics, [])

      assert plan.operations == []
      assert plan.reason == :balanced
    end

    test "detects imbalanced cluster" do
      metrics = %{
        stores: [
          %{
            node: "store-1",
            region_count: 12,
            regions: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            is_alive: true
          },
          %{node: "store-2", region_count: 4, regions: [13, 14, 15, 16], is_alive: true}
        ],
        total_regions: 16,
        timestamp: DateTime.utc_now()
      }

      plan = BalancePlanner.compute_plan(metrics, [])

      assert plan.operations != []
      assert plan.reason == :rebalance_needed

      op = hd(plan.operations)
      assert op.type == :move_region
      assert op.from_store == "store-1"
      assert op.to_store == "store-2"
    end

    test "skips planning when operations active" do
      metrics = %{
        stores: [
          %{node: "store-1", region_count: 10, regions: [], is_alive: true},
          %{node: "store-2", region_count: 2, regions: [], is_alive: true}
        ],
        total_regions: 12,
        timestamp: DateTime.utc_now()
      }

      active_ops = [%{type: :move_region}]

      plan = BalancePlanner.compute_plan(metrics, active_ops)

      assert plan.operations == []
      assert plan.reason == :skip
    end

    test "ignores dead stores" do
      metrics = %{
        stores: [
          %{node: "store-1", region_count: 8, regions: [1, 2, 3, 4, 5, 6, 7, 8], is_alive: true},
          %{
            node: "store-2",
            region_count: 8,
            regions: [9, 10, 11, 12, 13, 14, 15, 16],
            is_alive: true
          },
          # Dead
          %{node: "store-3", region_count: 10, regions: [], is_alive: false}
        ],
        total_regions: 26,
        timestamp: DateTime.utc_now()
      }

      plan = BalancePlanner.compute_plan(metrics, [])

      # Should only consider alive stores
      assert plan.operations == []
    end
  end
end
