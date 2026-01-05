defmodule PD.API.GRPCTest do
  use ExUnit.Case, async: true

  alias PD.API.GRPC.Cluster
  alias PD.API.GRPC.TSO
  alias PD.API.GRPC.Schema

  describe "module compilation and structure" do
    test "Cluster service module exists" do
      assert Code.ensure_loaded?(Cluster)
    end

    test "TSO service module exists" do
      assert Code.ensure_loaded?(TSO)
    end

    test "Schema service module exists" do
      assert Code.ensure_loaded?(Schema)
    end
  end

  describe "Cluster service has required functions" do
    test "has cluster management functions" do
      functions = Cluster.__info__(:functions)

      assert Keyword.has_key?(functions, :get_region)
      assert Keyword.has_key?(functions, :list_stores)
      assert Keyword.has_key?(functions, :register_store)
      assert Keyword.has_key?(functions, :heartbeat)
    end
  end

  describe "TSO service has required functions" do
    test "has timestamp oracle functions" do
      functions = TSO.__info__(:functions)

      assert Keyword.has_key?(functions, :get_timestamp)
    end
  end

  describe "Schema service has required functions" do
    test "has schema management functions" do
      functions = Schema.__info__(:functions)

      assert Keyword.has_key?(functions, :create_table)
      assert Keyword.has_key?(functions, :drop_table)
      assert Keyword.has_key?(functions, :create_index)
      assert Keyword.has_key?(functions, :drop_index)
    end
  end
end
