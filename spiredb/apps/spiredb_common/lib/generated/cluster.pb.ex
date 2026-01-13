defmodule Spiredb.Cluster.ColumnType do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "ColumnType",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_INT8",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_INT16",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_INT32",
          number: 2,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_INT64",
          number: 3,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_UINT8",
          number: 4,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_UINT16",
          number: 5,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_UINT32",
          number: 6,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_UINT64",
          number: 7,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_FLOAT32",
          number: 8,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_FLOAT64",
          number: 9,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_BOOL",
          number: 10,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_STRING",
          number: 11,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_BYTES",
          number: 12,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_DATE",
          number: 13,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_TIMESTAMP",
          number: 14,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_DECIMAL",
          number: 15,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_LIST",
          number: 16,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "TYPE_VECTOR",
          number: 17,
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

  field(:TYPE_INT8, 0)
  field(:TYPE_INT16, 1)
  field(:TYPE_INT32, 2)
  field(:TYPE_INT64, 3)
  field(:TYPE_UINT8, 4)
  field(:TYPE_UINT16, 5)
  field(:TYPE_UINT32, 6)
  field(:TYPE_UINT64, 7)
  field(:TYPE_FLOAT32, 8)
  field(:TYPE_FLOAT64, 9)
  field(:TYPE_BOOL, 10)
  field(:TYPE_STRING, 11)
  field(:TYPE_BYTES, 12)
  field(:TYPE_DATE, 13)
  field(:TYPE_TIMESTAMP, 14)
  field(:TYPE_DECIMAL, 15)
  field(:TYPE_LIST, 16)
  field(:TYPE_VECTOR, 17)
end

defmodule Spiredb.Cluster.IndexType do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "IndexType",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "INDEX_BTREE",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "INDEX_ANODE",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "INDEX_MANODE",
          number: 2,
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

  field(:INDEX_BTREE, 0)
  field(:INDEX_ANODE, 1)
  field(:INDEX_MANODE, 2)
end

defmodule Spiredb.Cluster.PeerRole do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "PeerRole",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PEER_FOLLOWER",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PEER_LEADER",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PEER_LEARNER",
          number: 2,
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

  field(:PEER_FOLLOWER, 0)
  field(:PEER_LEADER, 1)
  field(:PEER_LEARNER, 2)
end

defmodule Spiredb.Cluster.RegionState do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "RegionState",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "REGION_ACTIVE",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "REGION_SPLITTING",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "REGION_MERGING",
          number: 2,
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

  field(:REGION_ACTIVE, 0)
  field(:REGION_SPLITTING, 1)
  field(:REGION_MERGING, 2)
end

defmodule Spiredb.Cluster.StoreState do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "StoreState",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "STORE_UP",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "STORE_DOWN",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "STORE_TOMBSTONE",
          number: 2,
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

  field(:STORE_UP, 0)
  field(:STORE_DOWN, 1)
  field(:STORE_TOMBSTONE, 2)
end

defmodule Spiredb.Cluster.PluginType do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "PluginType",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PLUGIN_INDEX",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PLUGIN_STORAGE",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PLUGIN_FUNCTION",
          number: 2,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PLUGIN_PROTOCOL",
          number: 3,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PLUGIN_AUTH",
          number: 4,
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

  field(:PLUGIN_INDEX, 0)
  field(:PLUGIN_STORAGE, 1)
  field(:PLUGIN_FUNCTION, 2)
  field(:PLUGIN_PROTOCOL, 3)
  field(:PLUGIN_AUTH, 4)
end

defmodule Spiredb.Cluster.PluginState do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "PluginState",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PLUGIN_LOADED",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PLUGIN_ERROR",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "PLUGIN_DISABLED",
          number: 2,
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

  field(:PLUGIN_LOADED, 0)
  field(:PLUGIN_ERROR, 1)
  field(:PLUGIN_DISABLED, 2)
end

defmodule Spiredb.Cluster.InternalMutationType do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "InternalMutationType",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "INTERNAL_MUTATION_PUT",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "INTERNAL_MUTATION_DELETE",
          number: 1,
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

  field(:INTERNAL_MUTATION_PUT, 0)
  field(:INTERNAL_MUTATION_DELETE, 1)
end

defmodule Spiredb.Cluster.GetTimestampRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetTimestampRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "count",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "count",
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

  field(:count, 1, type: :uint32)
end

defmodule Spiredb.Cluster.GetTimestampResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetTimestampResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_ts",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTs",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "count",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "count",
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

  field(:start_ts, 1, type: :uint64, json_name: "startTs")
  field(:count, 2, type: :uint32)
end

defmodule Spiredb.Cluster.ColumnDef do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ColumnDef",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "type",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.ColumnType",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "type",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "nullable",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "nullable",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "default_value",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "defaultValue",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "precision",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "precision",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "scale",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "scale",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "vector_dim",
          extendee: nil,
          number: 7,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "vectorDim",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "list_elem",
          extendee: nil,
          number: 8,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.ColumnType",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "listElem",
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

  field(:name, 1, type: :string)
  field(:type, 2, type: Spiredb.Cluster.ColumnType, enum: true)
  field(:nullable, 3, type: :bool)
  field(:default_value, 4, type: :bytes, json_name: "defaultValue")
  field(:precision, 5, type: :uint32)
  field(:scale, 6, type: :uint32)
  field(:vector_dim, 7, type: :uint32, json_name: "vectorDim")
  field(:list_elem, 8, type: Spiredb.Cluster.ColumnType, json_name: "listElem", enum: true)
end

defmodule Spiredb.Cluster.CreateTableRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "CreateTableRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "columns",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.ColumnDef",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "columns",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "primary_key",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "primaryKey",
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

  field(:name, 1, type: :string)
  field(:columns, 2, repeated: true, type: Spiredb.Cluster.ColumnDef)
  field(:primary_key, 3, repeated: true, type: :string, json_name: "primaryKey")
end

defmodule Spiredb.Cluster.CreateTableResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "CreateTableResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "table_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "tableId",
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

  field(:table_id, 1, type: :uint64, json_name: "tableId")
end

defmodule Spiredb.Cluster.DropTableRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "DropTableRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
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

  field(:name, 1, type: :string)
end

defmodule Spiredb.Cluster.GetTableRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetTableRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "id",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [
        %Google.Protobuf.OneofDescriptorProto{
          name: "identifier",
          options: nil,
          __unknown_fields__: []
        }
      ],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  oneof(:identifier, 0)

  field(:id, 1, type: :uint64, oneof: 0)
  field(:name, 2, type: :string, oneof: 0)
end

defmodule Spiredb.Cluster.TableSchema do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "TableSchema",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "id",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "columns",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.ColumnDef",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "columns",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "primary_key",
          extendee: nil,
          number: 4,
          label: :LABEL_REPEATED,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "primaryKey",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_prefix",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionPrefix",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "created_at",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "createdAt",
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

  field(:id, 1, type: :uint64)
  field(:name, 2, type: :string)
  field(:columns, 3, repeated: true, type: Spiredb.Cluster.ColumnDef)
  field(:primary_key, 4, repeated: true, type: :string, json_name: "primaryKey")
  field(:region_prefix, 5, type: :string, json_name: "regionPrefix")
  field(:created_at, 6, type: :uint64, json_name: "createdAt")
end

defmodule Spiredb.Cluster.TableList do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "TableList",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "tables",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.TableSchema",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "tables",
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

  field(:tables, 1, repeated: true, type: Spiredb.Cluster.TableSchema)
end

defmodule Spiredb.Cluster.CreateIndexRequest.ParamsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ParamsEntry",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "key",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "key",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "value",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "value",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: %Google.Protobuf.MessageOptions{
        message_set_wire_format: false,
        no_standard_descriptor_accessor: false,
        deprecated: false,
        map_entry: true,
        deprecated_legacy_json_field_conflicts: nil,
        features: nil,
        uninterpreted_option: [],
        __pb_extensions__: %{},
        __unknown_fields__: []
      },
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spiredb.Cluster.CreateIndexRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "CreateIndexRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "table_name",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "tableName",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "type",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.IndexType",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "type",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "columns",
          extendee: nil,
          number: 4,
          label: :LABEL_REPEATED,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "columns",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "params",
          extendee: nil,
          number: 5,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.CreateIndexRequest.ParamsEntry",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "params",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [
        %Google.Protobuf.DescriptorProto{
          name: "ParamsEntry",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "key",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "key",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "value",
              extendee: nil,
              number: 2,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "value",
              proto3_optional: nil,
              __unknown_fields__: []
            }
          ],
          nested_type: [],
          enum_type: [],
          extension_range: [],
          extension: [],
          options: %Google.Protobuf.MessageOptions{
            message_set_wire_format: false,
            no_standard_descriptor_accessor: false,
            deprecated: false,
            map_entry: true,
            deprecated_legacy_json_field_conflicts: nil,
            features: nil,
            uninterpreted_option: [],
            __pb_extensions__: %{},
            __unknown_fields__: []
          },
          oneof_decl: [],
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        }
      ],
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

  field(:name, 1, type: :string)
  field(:table_name, 2, type: :string, json_name: "tableName")
  field(:type, 3, type: Spiredb.Cluster.IndexType, enum: true)
  field(:columns, 4, repeated: true, type: :string)

  field(:params, 5,
    repeated: true,
    type: Spiredb.Cluster.CreateIndexRequest.ParamsEntry,
    map: true
  )
end

defmodule Spiredb.Cluster.CreateIndexResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "CreateIndexResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "index_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "indexId",
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

  field(:index_id, 1, type: :uint64, json_name: "indexId")
end

defmodule Spiredb.Cluster.DropIndexRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "DropIndexRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
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

  field(:name, 1, type: :string)
end

defmodule Spiredb.Cluster.GetIndexRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetIndexRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
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

  field(:name, 1, type: :string)
end

defmodule Spiredb.Cluster.IndexSchema.ParamsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ParamsEntry",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "key",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "key",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "value",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "value",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: %Google.Protobuf.MessageOptions{
        message_set_wire_format: false,
        no_standard_descriptor_accessor: false,
        deprecated: false,
        map_entry: true,
        deprecated_legacy_json_field_conflicts: nil,
        features: nil,
        uninterpreted_option: [],
        __pb_extensions__: %{},
        __unknown_fields__: []
      },
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spiredb.Cluster.IndexSchema do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "IndexSchema",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "id",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "table_id",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "tableId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "type",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.IndexType",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "type",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "columns",
          extendee: nil,
          number: 5,
          label: :LABEL_REPEATED,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "columns",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "params",
          extendee: nil,
          number: 6,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.IndexSchema.ParamsEntry",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "params",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [
        %Google.Protobuf.DescriptorProto{
          name: "ParamsEntry",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "key",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "key",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "value",
              extendee: nil,
              number: 2,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "value",
              proto3_optional: nil,
              __unknown_fields__: []
            }
          ],
          nested_type: [],
          enum_type: [],
          extension_range: [],
          extension: [],
          options: %Google.Protobuf.MessageOptions{
            message_set_wire_format: false,
            no_standard_descriptor_accessor: false,
            deprecated: false,
            map_entry: true,
            deprecated_legacy_json_field_conflicts: nil,
            features: nil,
            uninterpreted_option: [],
            __pb_extensions__: %{},
            __unknown_fields__: []
          },
          oneof_decl: [],
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        }
      ],
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

  field(:id, 1, type: :uint64)
  field(:name, 2, type: :string)
  field(:table_id, 3, type: :uint64, json_name: "tableId")
  field(:type, 4, type: Spiredb.Cluster.IndexType, enum: true)
  field(:columns, 5, repeated: true, type: :string)
  field(:params, 6, repeated: true, type: Spiredb.Cluster.IndexSchema.ParamsEntry, map: true)
end

defmodule Spiredb.Cluster.ListIndexesRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ListIndexesRequest",
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

defmodule Spiredb.Cluster.IndexList do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "IndexList",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "indexes",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.IndexSchema",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "indexes",
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

  field(:indexes, 1, repeated: true, type: Spiredb.Cluster.IndexSchema)
end

defmodule Spiredb.Cluster.GetTableStatsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetTableStatsRequest",
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

defmodule Spiredb.Cluster.TableStats.ColumnStatsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ColumnStatsEntry",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "key",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "key",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "value",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.ColumnStats",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "value",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: %Google.Protobuf.MessageOptions{
        message_set_wire_format: false,
        no_standard_descriptor_accessor: false,
        deprecated: false,
        map_entry: true,
        deprecated_legacy_json_field_conflicts: nil,
        features: nil,
        uninterpreted_option: [],
        __pb_extensions__: %{},
        __unknown_fields__: []
      },
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:key, 1, type: :string)
  field(:value, 2, type: Spiredb.Cluster.ColumnStats)
end

defmodule Spiredb.Cluster.TableStats do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "TableStats",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "row_count",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "rowCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "size_bytes",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "sizeBytes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "last_updated",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "lastUpdated",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "column_stats",
          extendee: nil,
          number: 4,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.TableStats.ColumnStatsEntry",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "columnStats",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [
        %Google.Protobuf.DescriptorProto{
          name: "ColumnStatsEntry",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "key",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "key",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "value",
              extendee: nil,
              number: 2,
              label: :LABEL_OPTIONAL,
              type: :TYPE_MESSAGE,
              type_name: ".spiredb.cluster.ColumnStats",
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "value",
              proto3_optional: nil,
              __unknown_fields__: []
            }
          ],
          nested_type: [],
          enum_type: [],
          extension_range: [],
          extension: [],
          options: %Google.Protobuf.MessageOptions{
            message_set_wire_format: false,
            no_standard_descriptor_accessor: false,
            deprecated: false,
            map_entry: true,
            deprecated_legacy_json_field_conflicts: nil,
            features: nil,
            uninterpreted_option: [],
            __pb_extensions__: %{},
            __unknown_fields__: []
          },
          oneof_decl: [],
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        }
      ],
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

  field(:row_count, 1, type: :uint64, json_name: "rowCount")
  field(:size_bytes, 2, type: :uint64, json_name: "sizeBytes")
  field(:last_updated, 3, type: :uint64, json_name: "lastUpdated")

  field(:column_stats, 4,
    repeated: true,
    type: Spiredb.Cluster.TableStats.ColumnStatsEntry,
    json_name: "columnStats",
    map: true
  )
end

defmodule Spiredb.Cluster.ColumnStats do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ColumnStats",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "distinct_count",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "distinctCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "min_value",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "minValue",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "max_value",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "maxValue",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "null_count",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "nullCount",
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

  field(:distinct_count, 1, type: :uint64, json_name: "distinctCount")
  field(:min_value, 2, type: :bytes, json_name: "minValue")
  field(:max_value, 3, type: :bytes, json_name: "maxValue")
  field(:null_count, 4, type: :uint64, json_name: "nullCount")
end

defmodule Spiredb.Cluster.UpdateTableStatsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "UpdateTableStatsRequest",
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
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "row_count",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "rowCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "size_bytes",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "sizeBytes",
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
  field(:row_count, 2, type: :uint64, json_name: "rowCount")
  field(:size_bytes, 3, type: :uint64, json_name: "sizeBytes")
end

defmodule Spiredb.Cluster.GetRegionRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetRegionRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionId",
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

  field(:region_id, 1, type: :uint64, json_name: "regionId")
end

defmodule Spiredb.Cluster.GetRegionByKeyRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetRegionByKeyRequest",
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

defmodule Spiredb.Cluster.GetTableRegionsRequest do
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

defmodule Spiredb.Cluster.Region do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Region",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "id",
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
          name: "peers",
          extendee: nil,
          number: 4,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.Peer",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "peers",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "leader_store_id",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "leaderStoreId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_epoch",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionEpoch",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "state",
          extendee: nil,
          number: 7,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.RegionState",
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

  field(:id, 1, type: :uint64)
  field(:start_key, 2, type: :bytes, json_name: "startKey")
  field(:end_key, 3, type: :bytes, json_name: "endKey")
  field(:peers, 4, repeated: true, type: Spiredb.Cluster.Peer)
  field(:leader_store_id, 5, type: :uint64, json_name: "leaderStoreId")
  field(:region_epoch, 6, type: :uint64, json_name: "regionEpoch")
  field(:state, 7, type: Spiredb.Cluster.RegionState, enum: true)
end

defmodule Spiredb.Cluster.Peer do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Peer",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "store_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "storeId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "role",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.PeerRole",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "role",
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

  field(:store_id, 1, type: :uint64, json_name: "storeId")
  field(:role, 2, type: Spiredb.Cluster.PeerRole, enum: true)
end

defmodule Spiredb.Cluster.RegionList do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "RegionList",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "regions",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.Region",
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

  field(:regions, 1, repeated: true, type: Spiredb.Cluster.Region)
end

defmodule Spiredb.Cluster.GetStoreRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "GetStoreRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "store_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "storeId",
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

  field(:store_id, 1, type: :uint64, json_name: "storeId")
end

defmodule Spiredb.Cluster.Store.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "LabelsEntry",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "key",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "key",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "value",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "value",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: %Google.Protobuf.MessageOptions{
        message_set_wire_format: false,
        no_standard_descriptor_accessor: false,
        deprecated: false,
        map_entry: true,
        deprecated_legacy_json_field_conflicts: nil,
        features: nil,
        uninterpreted_option: [],
        __pb_extensions__: %{},
        __unknown_fields__: []
      },
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spiredb.Cluster.Store do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Store",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "id",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "address",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "address",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "state",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.StoreState",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "state",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "capacity",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "capacity",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "available",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "available",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_count",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "labels",
          extendee: nil,
          number: 7,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.Store.LabelsEntry",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "labels",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [
        %Google.Protobuf.DescriptorProto{
          name: "LabelsEntry",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "key",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "key",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "value",
              extendee: nil,
              number: 2,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "value",
              proto3_optional: nil,
              __unknown_fields__: []
            }
          ],
          nested_type: [],
          enum_type: [],
          extension_range: [],
          extension: [],
          options: %Google.Protobuf.MessageOptions{
            message_set_wire_format: false,
            no_standard_descriptor_accessor: false,
            deprecated: false,
            map_entry: true,
            deprecated_legacy_json_field_conflicts: nil,
            features: nil,
            uninterpreted_option: [],
            __pb_extensions__: %{},
            __unknown_fields__: []
          },
          oneof_decl: [],
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        }
      ],
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

  field(:id, 1, type: :uint64)
  field(:address, 2, type: :string)
  field(:state, 3, type: Spiredb.Cluster.StoreState, enum: true)
  field(:capacity, 4, type: :uint64)
  field(:available, 5, type: :uint64)
  field(:region_count, 6, type: :uint32, json_name: "regionCount")
  field(:labels, 7, repeated: true, type: Spiredb.Cluster.Store.LabelsEntry, map: true)
end

defmodule Spiredb.Cluster.StoreList do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "StoreList",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "stores",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.Store",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "stores",
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

  field(:stores, 1, repeated: true, type: Spiredb.Cluster.Store)
end

defmodule Spiredb.Cluster.RegisterStoreRequest.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "LabelsEntry",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "key",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "key",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "value",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "value",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: %Google.Protobuf.MessageOptions{
        message_set_wire_format: false,
        no_standard_descriptor_accessor: false,
        deprecated: false,
        map_entry: true,
        deprecated_legacy_json_field_conflicts: nil,
        features: nil,
        uninterpreted_option: [],
        __pb_extensions__: %{},
        __unknown_fields__: []
      },
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Spiredb.Cluster.RegisterStoreRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "RegisterStoreRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "address",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "address",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "capacity",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "capacity",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "labels",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.RegisterStoreRequest.LabelsEntry",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "labels",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [
        %Google.Protobuf.DescriptorProto{
          name: "LabelsEntry",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "key",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "key",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "value",
              extendee: nil,
              number: 2,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "value",
              proto3_optional: nil,
              __unknown_fields__: []
            }
          ],
          nested_type: [],
          enum_type: [],
          extension_range: [],
          extension: [],
          options: %Google.Protobuf.MessageOptions{
            message_set_wire_format: false,
            no_standard_descriptor_accessor: false,
            deprecated: false,
            map_entry: true,
            deprecated_legacy_json_field_conflicts: nil,
            features: nil,
            uninterpreted_option: [],
            __pb_extensions__: %{},
            __unknown_fields__: []
          },
          oneof_decl: [],
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        }
      ],
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

  field(:address, 1, type: :string)
  field(:capacity, 2, type: :uint64)

  field(:labels, 3,
    repeated: true,
    type: Spiredb.Cluster.RegisterStoreRequest.LabelsEntry,
    map: true
  )
end

defmodule Spiredb.Cluster.RegisterStoreResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "RegisterStoreResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "store_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "storeId",
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

  field(:store_id, 1, type: :uint64, json_name: "storeId")
end

defmodule Spiredb.Cluster.StoreHeartbeat do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "StoreHeartbeat",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "store_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "storeId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "address",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "address",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "available",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "available",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_count",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_stats",
          extendee: nil,
          number: 5,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.RegionStats",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionStats",
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

  field(:store_id, 1, type: :uint64, json_name: "storeId")
  field(:address, 2, type: :string)
  field(:available, 3, type: :uint64)
  field(:region_count, 4, type: :uint32, json_name: "regionCount")

  field(:region_stats, 5,
    repeated: true,
    type: Spiredb.Cluster.RegionStats,
    json_name: "regionStats"
  )
end

defmodule Spiredb.Cluster.RegionStats do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "RegionStats",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "approximate_size",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "approximateSize",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "approximate_keys",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "approximateKeys",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "qps",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "qps",
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

  field(:region_id, 1, type: :uint64, json_name: "regionId")
  field(:approximate_size, 2, type: :uint64, json_name: "approximateSize")
  field(:approximate_keys, 3, type: :uint64, json_name: "approximateKeys")
  field(:qps, 4, type: :uint64)
end

defmodule Spiredb.Cluster.StoreHeartbeatResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "StoreHeartbeatResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "tasks",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.ScheduledTask",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "tasks",
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

  field(:tasks, 1, repeated: true, type: Spiredb.Cluster.ScheduledTask)
end

defmodule Spiredb.Cluster.ScheduledTask do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ScheduledTask",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "task_id",
          extendee: nil,
          number: 7,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "taskId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "leader_epoch",
          extendee: nil,
          number: 8,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "leaderEpoch",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "split",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.SplitRegion",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "split",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "merge",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.MergeRegions",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "merge",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "transfer_leader",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.TransferLeader",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "transferLeader",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "add_peer",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.AddPeer",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "addPeer",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "remove_peer",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.RemovePeer",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "removePeer",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "compact",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.CompactRegion",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "compact",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [
        %Google.Protobuf.OneofDescriptorProto{name: "task", options: nil, __unknown_fields__: []}
      ],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  oneof(:task, 0)

  field(:task_id, 7, type: :uint64, json_name: "taskId")
  field(:leader_epoch, 8, type: :uint64, json_name: "leaderEpoch")
  field(:split, 1, type: Spiredb.Cluster.SplitRegion, oneof: 0)
  field(:merge, 2, type: Spiredb.Cluster.MergeRegions, oneof: 0)

  field(:transfer_leader, 3,
    type: Spiredb.Cluster.TransferLeader,
    json_name: "transferLeader",
    oneof: 0
  )

  field(:add_peer, 4, type: Spiredb.Cluster.AddPeer, json_name: "addPeer", oneof: 0)
  field(:remove_peer, 5, type: Spiredb.Cluster.RemovePeer, json_name: "removePeer", oneof: 0)
  field(:compact, 6, type: Spiredb.Cluster.CompactRegion, oneof: 0)
end

defmodule Spiredb.Cluster.SplitRegion do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "SplitRegion",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "split_key",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "splitKey",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "new_region_id",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "newRegionId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "new_peer_id",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "newPeerId",
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

  field(:region_id, 1, type: :uint64, json_name: "regionId")
  field(:split_key, 2, type: :bytes, json_name: "splitKey")
  field(:new_region_id, 3, type: :uint64, json_name: "newRegionId")
  field(:new_peer_id, 4, type: :uint64, json_name: "newPeerId")
end

defmodule Spiredb.Cluster.MergeRegions do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "MergeRegions",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "source_region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "sourceRegionId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "target_region_id",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "targetRegionId",
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

  field(:source_region_id, 1, type: :uint64, json_name: "sourceRegionId")
  field(:target_region_id, 2, type: :uint64, json_name: "targetRegionId")
end

defmodule Spiredb.Cluster.TransferLeader do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "TransferLeader",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "from_store_id",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "fromStoreId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "to_store_id",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "toStoreId",
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

  field(:region_id, 1, type: :uint64, json_name: "regionId")
  field(:from_store_id, 2, type: :uint64, json_name: "fromStoreId")
  field(:to_store_id, 3, type: :uint64, json_name: "toStoreId")
end

defmodule Spiredb.Cluster.AddPeer do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "AddPeer",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "store_id",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "storeId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "peer_id",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "peerId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "is_learner",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "isLearner",
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

  field(:region_id, 1, type: :uint64, json_name: "regionId")
  field(:store_id, 2, type: :uint64, json_name: "storeId")
  field(:peer_id, 3, type: :uint64, json_name: "peerId")
  field(:is_learner, 4, type: :bool, json_name: "isLearner")
end

defmodule Spiredb.Cluster.RemovePeer do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "RemovePeer",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "regionId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "store_id",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "storeId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "peer_id",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "peerId",
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

  field(:region_id, 1, type: :uint64, json_name: "regionId")
  field(:store_id, 2, type: :uint64, json_name: "storeId")
  field(:peer_id, 3, type: :uint64, json_name: "peerId")
end

defmodule Spiredb.Cluster.CompactRegion do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "CompactRegion",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "region_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
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

  field(:region_id, 1, type: :uint64, json_name: "regionId")
  field(:start_key, 2, type: :bytes, json_name: "startKey")
  field(:end_key, 3, type: :bytes, json_name: "endKey")
end

defmodule Spiredb.Cluster.InstallPluginRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "InstallPluginRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "hex_package",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "hexPackage",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "github_repo",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "githubRepo",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "tarball",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "tarball",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [
        %Google.Protobuf.OneofDescriptorProto{
          name: "source",
          options: nil,
          __unknown_fields__: []
        }
      ],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  oneof(:source, 0)

  field(:hex_package, 1, type: :string, json_name: "hexPackage", oneof: 0)
  field(:github_repo, 2, type: :string, json_name: "githubRepo", oneof: 0)
  field(:tarball, 3, type: :bytes, oneof: 0)
end

defmodule Spiredb.Cluster.InstallPluginResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "InstallPluginResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "version",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "version",
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

  field(:name, 1, type: :string)
  field(:version, 2, type: :string)
end

defmodule Spiredb.Cluster.UninstallPluginRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "UninstallPluginRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
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

  field(:name, 1, type: :string)
end

defmodule Spiredb.Cluster.PluginInfo do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "PluginInfo",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "version",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "version",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "type",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.PluginType",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "type",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "has_nif",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "hasNif",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "state",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.PluginState",
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

  field(:name, 1, type: :string)
  field(:version, 2, type: :string)
  field(:type, 3, type: Spiredb.Cluster.PluginType, enum: true)
  field(:has_nif, 4, type: :bool, json_name: "hasNif")
  field(:state, 5, type: Spiredb.Cluster.PluginState, enum: true)
end

defmodule Spiredb.Cluster.PluginList do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "PluginList",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "plugins",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.PluginInfo",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "plugins",
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

  field(:plugins, 1, repeated: true, type: Spiredb.Cluster.PluginInfo)
end

defmodule Spiredb.Cluster.ReloadPluginRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ReloadPluginRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
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

  field(:name, 1, type: :string)
end

defmodule Spiredb.Cluster.AsyncPrewriteRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "AsyncPrewriteRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "mutations",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.InternalMutation",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "mutations",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "primary_key",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "primaryKey",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "secondary_keys",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "secondaryKeys",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_ts",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTs",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "min_commit_ts",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "minCommitTs",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "lock_ttl",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "lockTtl",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "txn_id",
          extendee: nil,
          number: 7,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "txnId",
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

  field(:mutations, 1, repeated: true, type: Spiredb.Cluster.InternalMutation)
  field(:primary_key, 2, type: :bytes, json_name: "primaryKey")
  field(:secondary_keys, 3, repeated: true, type: :bytes, json_name: "secondaryKeys")
  field(:start_ts, 4, type: :uint64, json_name: "startTs")
  field(:min_commit_ts, 5, type: :uint64, json_name: "minCommitTs")
  field(:lock_ttl, 6, type: :uint64, json_name: "lockTtl")
  field(:txn_id, 7, type: :bytes, json_name: "txnId")
end

defmodule Spiredb.Cluster.InternalMutation do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "InternalMutation",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "type",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".spiredb.cluster.InternalMutationType",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "type",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "key",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "key",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "value",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "value",
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

  field(:type, 1, type: Spiredb.Cluster.InternalMutationType, enum: true)
  field(:key, 2, type: :bytes)
  field(:value, 3, type: :bytes)
end

defmodule Spiredb.Cluster.AsyncPrewriteResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "AsyncPrewriteResponse",
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
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "errors",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.InternalKeyError",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "errors",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "actual_min_commit_ts",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "actualMinCommitTs",
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
  field(:errors, 2, repeated: true, type: Spiredb.Cluster.InternalKeyError)
  field(:actual_min_commit_ts, 3, type: :uint64, json_name: "actualMinCommitTs")
end

defmodule Spiredb.Cluster.InternalKeyError do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "InternalKeyError",
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
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "error",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "error",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "lock_info",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.InternalLockInfo",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "lockInfo",
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
  field(:error, 2, type: :string)
  field(:lock_info, 3, type: Spiredb.Cluster.InternalLockInfo, json_name: "lockInfo")
end

defmodule Spiredb.Cluster.InternalLockInfo do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "InternalLockInfo",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "primary_key",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "primaryKey",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_ts",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTs",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "ttl",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "ttl",
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

  field(:primary_key, 1, type: :bytes, json_name: "primaryKey")
  field(:start_ts, 2, type: :uint64, json_name: "startTs")
  field(:ttl, 3, type: :uint64)
end

defmodule Spiredb.Cluster.CheckSecondaryLocksRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "CheckSecondaryLocksRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "primary_key",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "primaryKey",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_ts",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTs",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "secondary_keys",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "secondaryKeys",
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

  field(:primary_key, 1, type: :bytes, json_name: "primaryKey")
  field(:start_ts, 2, type: :uint64, json_name: "startTs")
  field(:secondary_keys, 3, repeated: true, type: :bytes, json_name: "secondaryKeys")
end

defmodule Spiredb.Cluster.CheckSecondaryLocksResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "CheckSecondaryLocksResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "statuses",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".spiredb.cluster.SecondaryLockStatus",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "statuses",
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

  field(:statuses, 1, repeated: true, type: Spiredb.Cluster.SecondaryLockStatus)
end

defmodule Spiredb.Cluster.SecondaryLockStatus do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "SecondaryLockStatus",
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
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "locked",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "locked",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "committed",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "committed",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "commit_ts",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "commitTs",
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
  field(:locked, 2, type: :bool)
  field(:committed, 3, type: :bool)
  field(:commit_ts, 4, type: :uint64, json_name: "commitTs")
end

defmodule Spiredb.Cluster.InternalResolveLockRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "InternalResolveLockRequest",
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
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_ts",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTs",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "commit_ts",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "commitTs",
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
  field(:start_ts, 2, type: :uint64, json_name: "startTs")
  field(:commit_ts, 3, type: :uint64, json_name: "commitTs")
end

defmodule Spiredb.Cluster.Empty do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Empty",
      field: [],
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
end

defmodule Spiredb.Cluster.TSOService.Service do
  @moduledoc false

  use GRPC.Service, name: "spiredb.cluster.TSOService", protoc_gen_elixir_version: "0.15.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "TSOService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetTimestamp",
          input_type: ".spiredb.cluster.GetTimestampRequest",
          output_type: ".spiredb.cluster.GetTimestampResponse",
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

  rpc(:GetTimestamp, Spiredb.Cluster.GetTimestampRequest, Spiredb.Cluster.GetTimestampResponse)
end

defmodule Spiredb.Cluster.TSOService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Spiredb.Cluster.TSOService.Service
end

defmodule Spiredb.Cluster.SchemaService.Service do
  @moduledoc false

  use GRPC.Service, name: "spiredb.cluster.SchemaService", protoc_gen_elixir_version: "0.15.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "SchemaService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "CreateTable",
          input_type: ".spiredb.cluster.CreateTableRequest",
          output_type: ".spiredb.cluster.CreateTableResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "DropTable",
          input_type: ".spiredb.cluster.DropTableRequest",
          output_type: ".spiredb.cluster.Empty",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetTable",
          input_type: ".spiredb.cluster.GetTableRequest",
          output_type: ".spiredb.cluster.TableSchema",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "ListTables",
          input_type: ".spiredb.cluster.Empty",
          output_type: ".spiredb.cluster.TableList",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "CreateIndex",
          input_type: ".spiredb.cluster.CreateIndexRequest",
          output_type: ".spiredb.cluster.CreateIndexResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "DropIndex",
          input_type: ".spiredb.cluster.DropIndexRequest",
          output_type: ".spiredb.cluster.Empty",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetIndex",
          input_type: ".spiredb.cluster.GetIndexRequest",
          output_type: ".spiredb.cluster.IndexSchema",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "ListIndexes",
          input_type: ".spiredb.cluster.ListIndexesRequest",
          output_type: ".spiredb.cluster.IndexList",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetTableStats",
          input_type: ".spiredb.cluster.GetTableStatsRequest",
          output_type: ".spiredb.cluster.TableStats",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "UpdateTableStats",
          input_type: ".spiredb.cluster.UpdateTableStatsRequest",
          output_type: ".spiredb.cluster.Empty",
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

  rpc(:CreateTable, Spiredb.Cluster.CreateTableRequest, Spiredb.Cluster.CreateTableResponse)

  rpc(:DropTable, Spiredb.Cluster.DropTableRequest, Spiredb.Cluster.Empty)

  rpc(:GetTable, Spiredb.Cluster.GetTableRequest, Spiredb.Cluster.TableSchema)

  rpc(:ListTables, Spiredb.Cluster.Empty, Spiredb.Cluster.TableList)

  rpc(:CreateIndex, Spiredb.Cluster.CreateIndexRequest, Spiredb.Cluster.CreateIndexResponse)

  rpc(:DropIndex, Spiredb.Cluster.DropIndexRequest, Spiredb.Cluster.Empty)

  rpc(:GetIndex, Spiredb.Cluster.GetIndexRequest, Spiredb.Cluster.IndexSchema)

  rpc(:ListIndexes, Spiredb.Cluster.ListIndexesRequest, Spiredb.Cluster.IndexList)

  rpc(:GetTableStats, Spiredb.Cluster.GetTableStatsRequest, Spiredb.Cluster.TableStats)

  rpc(:UpdateTableStats, Spiredb.Cluster.UpdateTableStatsRequest, Spiredb.Cluster.Empty)
end

defmodule Spiredb.Cluster.SchemaService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Spiredb.Cluster.SchemaService.Service
end

defmodule Spiredb.Cluster.ClusterService.Service do
  @moduledoc false

  use GRPC.Service, name: "spiredb.cluster.ClusterService", protoc_gen_elixir_version: "0.15.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "ClusterService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetRegion",
          input_type: ".spiredb.cluster.GetRegionRequest",
          output_type: ".spiredb.cluster.Region",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetRegionByKey",
          input_type: ".spiredb.cluster.GetRegionByKeyRequest",
          output_type: ".spiredb.cluster.Region",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetTableRegions",
          input_type: ".spiredb.cluster.GetTableRegionsRequest",
          output_type: ".spiredb.cluster.RegionList",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "GetStore",
          input_type: ".spiredb.cluster.GetStoreRequest",
          output_type: ".spiredb.cluster.Store",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "ListStores",
          input_type: ".spiredb.cluster.Empty",
          output_type: ".spiredb.cluster.StoreList",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "RegisterStore",
          input_type: ".spiredb.cluster.RegisterStoreRequest",
          output_type: ".spiredb.cluster.RegisterStoreResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "Heartbeat",
          input_type: ".spiredb.cluster.StoreHeartbeat",
          output_type: ".spiredb.cluster.StoreHeartbeatResponse",
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

  rpc(:GetRegion, Spiredb.Cluster.GetRegionRequest, Spiredb.Cluster.Region)

  rpc(:GetRegionByKey, Spiredb.Cluster.GetRegionByKeyRequest, Spiredb.Cluster.Region)

  rpc(:GetTableRegions, Spiredb.Cluster.GetTableRegionsRequest, Spiredb.Cluster.RegionList)

  rpc(:GetStore, Spiredb.Cluster.GetStoreRequest, Spiredb.Cluster.Store)

  rpc(:ListStores, Spiredb.Cluster.Empty, Spiredb.Cluster.StoreList)

  rpc(:RegisterStore, Spiredb.Cluster.RegisterStoreRequest, Spiredb.Cluster.RegisterStoreResponse)

  rpc(:Heartbeat, Spiredb.Cluster.StoreHeartbeat, Spiredb.Cluster.StoreHeartbeatResponse)
end

defmodule Spiredb.Cluster.ClusterService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Spiredb.Cluster.ClusterService.Service
end

defmodule Spiredb.Cluster.PluginService.Service do
  @moduledoc false

  use GRPC.Service, name: "spiredb.cluster.PluginService", protoc_gen_elixir_version: "0.15.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "PluginService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "InstallPlugin",
          input_type: ".spiredb.cluster.InstallPluginRequest",
          output_type: ".spiredb.cluster.InstallPluginResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "UninstallPlugin",
          input_type: ".spiredb.cluster.UninstallPluginRequest",
          output_type: ".spiredb.cluster.Empty",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "ListPlugins",
          input_type: ".spiredb.cluster.Empty",
          output_type: ".spiredb.cluster.PluginList",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "ReloadPlugin",
          input_type: ".spiredb.cluster.ReloadPluginRequest",
          output_type: ".spiredb.cluster.Empty",
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

  rpc(:InstallPlugin, Spiredb.Cluster.InstallPluginRequest, Spiredb.Cluster.InstallPluginResponse)

  rpc(:UninstallPlugin, Spiredb.Cluster.UninstallPluginRequest, Spiredb.Cluster.Empty)

  rpc(:ListPlugins, Spiredb.Cluster.Empty, Spiredb.Cluster.PluginList)

  rpc(:ReloadPlugin, Spiredb.Cluster.ReloadPluginRequest, Spiredb.Cluster.Empty)
end

defmodule Spiredb.Cluster.PluginService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Spiredb.Cluster.PluginService.Service
end

defmodule Spiredb.Cluster.InternalTransactionService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "spiredb.cluster.InternalTransactionService",
    protoc_gen_elixir_version: "0.15.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "InternalTransactionService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "AsyncPrewrite",
          input_type: ".spiredb.cluster.AsyncPrewriteRequest",
          output_type: ".spiredb.cluster.AsyncPrewriteResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "CheckSecondaryLocks",
          input_type: ".spiredb.cluster.CheckSecondaryLocksRequest",
          output_type: ".spiredb.cluster.CheckSecondaryLocksResponse",
          options: nil,
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        },
        %Google.Protobuf.MethodDescriptorProto{
          name: "ResolveLock",
          input_type: ".spiredb.cluster.InternalResolveLockRequest",
          output_type: ".spiredb.cluster.Empty",
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

  rpc(:AsyncPrewrite, Spiredb.Cluster.AsyncPrewriteRequest, Spiredb.Cluster.AsyncPrewriteResponse)

  rpc(
    :CheckSecondaryLocks,
    Spiredb.Cluster.CheckSecondaryLocksRequest,
    Spiredb.Cluster.CheckSecondaryLocksResponse
  )

  rpc(:ResolveLock, Spiredb.Cluster.InternalResolveLockRequest, Spiredb.Cluster.Empty)
end

defmodule Spiredb.Cluster.InternalTransactionService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Spiredb.Cluster.InternalTransactionService.Service
end
