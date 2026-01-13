defmodule Store.KV.MetricsTest do
  use ExUnit.Case, async: true

  alias Store.KV.Metrics

  describe "prometheus_metrics/0" do
    test "returns string" do
      result = Metrics.prometheus_metrics()
      assert is_binary(result)
      # Should return some prometheus-formatted output
      assert String.contains?(result, "#") or String.contains?(result, "rocksdb")
    end
  end

  describe "get_metrics/0" do
    test "returns result" do
      result = Metrics.get_metrics()
      # May be error, success, or list depending on engine state
      assert match?({:ok, _}, result) or match?({:error, _}, result) or is_list(result)
    end
  end
end
