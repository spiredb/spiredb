defmodule SpireDb.Spiredb.Pd.RegionState do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "RegionState",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "REGION_STATE_UNSPECIFIED",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "REGION_STATE_ACTIVE",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "REGION_STATE_SPLITTING",
          number: 2,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "REGION_STATE_MERGING",
          number: 3,
          options: nil,
          __unknown_fields__: []
        }
      ],
      options: nil,
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:REGION_STATE_UNSPECIFIED, 0)
  field(:REGION_STATE_ACTIVE, 1)
  field(:REGION_STATE_SPLITTING, 2)
  field(:REGION_STATE_MERGING, 3)
end

defmodule SpireDb.Spiredb.Pd.GetTableRegionsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetTableRegionsRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "table_name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "tableName",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:table_name, 1, type: :string, json_name: "tableName")
end

defmodule SpireDb.Spiredb.Pd.GetTableRegionsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetTableRegionsResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "regions",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.pd.Region",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regions",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:regions, 1, repeated: true, type: SpireDb.Spiredb.Pd.Region)
end

defmodule SpireDb.Spiredb.Pd.GetRegionRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetRegionRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "key",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "key",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:key, 1, type: :bytes)
end

defmodule SpireDb.Spiredb.Pd.Region do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Region",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_key",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startKey",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "end_key",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "endKey",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "leader_node",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "leaderNode",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "followers",
          extendee: nil,
          number: 5,
          label: :LABEL_REPEATED,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "followers",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "state",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.pd.RegionState",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "state",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:region_id, 1, type: :uint32, json_name: "regionId")
  field(:start_key, 2, type: :bytes, json_name: "startKey")
  field(:end_key, 3, type: :bytes, json_name: "endKey")
  field(:leader_node, 4, type: :string, json_name: "leaderNode")
  field(:followers, 5, repeated: true, type: :string)
  field(:state, 6, type: SpireDb.Spiredb.Pd.RegionState, enum: true)
end

defmodule SpireDb.Spiredb.Pd.RegisterStoreRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "RegisterStoreRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "node_name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "nodeName",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:node_name, 1, type: :string, json_name: "nodeName")
end

defmodule SpireDb.Spiredb.Pd.RegisterStoreResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "RegisterStoreResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "success",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "success",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:success, 1, type: :bool)
end

defmodule SpireDb.Spiredb.Pd.HeartbeatRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "HeartbeatRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "node_name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "nodeName",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:node_name, 1, type: :string, json_name: "nodeName")
end

defmodule SpireDb.Spiredb.Pd.HeartbeatResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "HeartbeatResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "success",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "success",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:success, 1, type: :bool)
end

defmodule SpireDb.Spiredb.Pd.PlacementDriver.Service do
  @moduledoc false

  use GRPC.Service, name: "spiredb.pd.PlacementDriver", protoc_gen_elixir_version: "0.15.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "PlacementDriver",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetTableRegions",
          input_type: ".spiredb.pd.GetTableRegionsRequest",
          output_type: ".spiredb.pd.GetTableRegionsResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetRegion",
          input_type: ".spiredb.pd.GetRegionRequest",
          output_type: ".spiredb.pd.Region",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "RegisterStore",
          input_type: ".spiredb.pd.RegisterStoreRequest",
          output_type: ".spiredb.pd.RegisterStoreResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "Heartbeat",
          input_type: ".spiredb.pd.HeartbeatRequest",
          output_type: ".spiredb.pd.HeartbeatResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        }
      ],
      options: nil,
      __unknown_fields__: []
    }
  end

  rpc(
    :GetTableRegions,
    SpireDb.Spiredb.Pd.GetTableRegionsRequest,
    SpireDb.Spiredb.Pd.GetTableRegionsResponse
  )

  rpc(:GetRegion, SpireDb.Spiredb.Pd.GetRegionRequest, SpireDb.Spiredb.Pd.Region)

  rpc(
    :RegisterStore,
    SpireDb.Spiredb.Pd.RegisterStoreRequest,
    SpireDb.Spiredb.Pd.RegisterStoreResponse
  )

  rpc(:Heartbeat, SpireDb.Spiredb.Pd.HeartbeatRequest, SpireDb.Spiredb.Pd.HeartbeatResponse)
end

defmodule SpireDb.Spiredb.Pd.PlacementDriver.Stub do
  @moduledoc false

  use GRPC.Stub, service: SpireDb.Spiredb.Pd.PlacementDriver.Service
end
