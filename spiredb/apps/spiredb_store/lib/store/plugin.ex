defmodule Store.Plugin do
  @moduledoc """
  Plugin behaviour for SpireDB extensions.

  Supported plugin types:
  - :index - Custom index implementations
  - :storage - Storage engine extensions
  - :function - Custom SQL functions
  - :protocol - Protocol extensions (RESP, gRPC)
  - :auth - Authentication/authorization
  """

  @type plugin_type :: :index | :storage | :function | :protocol | :auth

  @type plugin_info :: %{
          name: String.t(),
          version: String.t(),
          type: plugin_type(),
          description: String.t(),
          has_nif: boolean()
        }

  @doc """
  Return plugin metadata.
  """
  @callback info() :: plugin_info()

  @doc """
  Initialize the plugin.
  """
  @callback init(opts :: keyword()) :: {:ok, state :: term()} | {:error, reason :: term()}

  @doc """
  Shutdown the plugin.
  """
  @callback shutdown(state :: term()) :: :ok

  @doc """
  Handle a custom command (for protocol plugins).
  """
  @callback handle_command(command :: list(), state :: term()) ::
              {:ok, result :: term(), new_state :: term()}
              | {:error, reason :: term()}

  @optional_callbacks handle_command: 2
end

defmodule Store.Plugin.Index do
  @moduledoc """
  Behaviour for custom index plugins.
  """

  @doc """
  Create a new index.
  """
  @callback create(name :: String.t(), opts :: keyword()) :: :ok | {:error, term()}

  @doc """
  Drop an index.
  """
  @callback drop(name :: String.t()) :: :ok | {:error, term()}

  @doc """
  Insert data into the index.
  """
  @callback insert(name :: String.t(), id :: term(), data :: term()) :: :ok | {:error, term()}

  @doc """
  Delete from the index.
  """
  @callback delete(name :: String.t(), id :: term()) :: :ok | {:error, term()}

  @doc """
  Search the index.
  """
  @callback search(name :: String.t(), query :: term(), opts :: keyword()) ::
              {:ok, results :: list()} | {:error, term()}
end

defmodule Store.Plugin.Function do
  @moduledoc """
  Behaviour for custom SQL function plugins.
  """

  @doc """
  Return function definitions.
  Each function is {name, arity, return_type}.
  """
  @callback functions() :: [{String.t(), non_neg_integer(), atom()}]

  @doc """
  Execute a function.
  """
  @callback execute(name :: String.t(), args :: list()) ::
              {:ok, term()} | {:error, term()}
end
