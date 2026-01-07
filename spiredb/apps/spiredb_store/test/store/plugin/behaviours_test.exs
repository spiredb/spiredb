defmodule Store.Plugin.BehavioursTest do
  use ExUnit.Case, async: true

  describe "Store.Plugin.StorageEngine behaviour" do
    test "defines required callbacks" do
      callbacks = Store.Plugin.StorageEngine.behaviour_info(:callbacks)

      assert {:init, 1} in callbacks
      assert {:get, 2} in callbacks
      assert {:put, 3} in callbacks
      assert {:delete, 2} in callbacks
      assert {:scan, 4} in callbacks
      assert {:shutdown, 1} in callbacks
    end

    test "defines optional callbacks" do
      optional = Store.Plugin.StorageEngine.behaviour_info(:optional_callbacks)
      assert {:flush, 1} in optional
    end
  end

  describe "Store.Plugin.Protocol behaviour" do
    test "defines required callbacks" do
      callbacks = Store.Plugin.Protocol.behaviour_info(:callbacks)

      assert {:commands, 0} in callbacks
      assert {:parse, 2} in callbacks
      assert {:execute, 2} in callbacks
      assert {:encode, 1} in callbacks
    end

    test "defines optional callbacks" do
      optional = Store.Plugin.Protocol.behaviour_info(:optional_callbacks)
      assert {:on_connect, 1} in optional
      assert {:on_disconnect, 1} in optional
    end
  end

  describe "Store.Plugin.Auth behaviour" do
    test "defines required callbacks" do
      callbacks = Store.Plugin.Auth.behaviour_info(:callbacks)

      assert {:authenticate, 1} in callbacks
      assert {:authorize, 3} in callbacks
    end

    test "defines optional callbacks" do
      optional = Store.Plugin.Auth.behaviour_info(:optional_callbacks)
      assert {:refresh, 1} in optional
      assert {:invalidate, 1} in optional
      assert {:has_permission?, 2} in optional
    end
  end

  describe "Store.Plugin.Index behaviour" do
    test "defines required callbacks" do
      callbacks = Store.Plugin.Index.behaviour_info(:callbacks)

      assert {:create, 2} in callbacks
      assert {:drop, 1} in callbacks
      assert {:insert, 3} in callbacks
      assert {:delete, 2} in callbacks
      assert {:search, 3} in callbacks
    end
  end

  describe "Store.Plugin.Function behaviour" do
    test "defines required callbacks" do
      callbacks = Store.Plugin.Function.behaviour_info(:callbacks)

      assert {:functions, 0} in callbacks
      assert {:execute, 2} in callbacks
    end
  end

  describe "Store.Plugin base behaviour" do
    test "defines required callbacks" do
      callbacks = Store.Plugin.behaviour_info(:callbacks)

      assert {:info, 0} in callbacks
      assert {:init, 1} in callbacks
      assert {:shutdown, 1} in callbacks
    end

    test "defines optional callbacks" do
      optional = Store.Plugin.behaviour_info(:optional_callbacks)
      assert {:handle_command, 2} in optional
    end
  end
end
