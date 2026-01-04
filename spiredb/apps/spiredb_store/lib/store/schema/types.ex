defmodule Store.Schema.Types do
  @moduledoc """
  Delegate to Common.Schema.Types for backward compatibility.
  """

  defdelegate from_proto(proto_type), to: Common.Schema.Types
  defdelegate to_proto(type), to: Common.Schema.Types
  defdelegate to_arrow(type, opts \\ []), to: Common.Schema.Types
  defdelegate from_resp(type_str), to: Common.Schema.Types
  defdelegate to_resp(type, opts \\ []), to: Common.Schema.Types
  defdelegate byte_size(type), to: Common.Schema.Types
  defdelegate numeric?(type), to: Common.Schema.Types
  defdelegate signed_integer?(type), to: Common.Schema.Types
  defdelegate unsigned_integer?(type), to: Common.Schema.Types
  defdelegate float?(type), to: Common.Schema.Types
end
