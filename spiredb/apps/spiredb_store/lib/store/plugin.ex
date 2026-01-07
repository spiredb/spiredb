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

defmodule Store.Plugin.StorageEngine do
  @moduledoc """
  Behaviour for custom storage engine plugins.

  Allows extending SpireDB with custom storage backends or
  storage features (e.g., encryption, tiered storage).
  """

  @doc """
  Initialize the storage engine.
  """
  @callback init(opts :: keyword()) :: {:ok, state :: term()} | {:error, term()}

  @doc """
  Read a value from storage.
  """
  @callback get(key :: binary(), state :: term()) ::
              {:ok, value :: binary()} | {:error, :not_found | term()}

  @doc """
  Write a value to storage.
  """
  @callback put(key :: binary(), value :: binary(), state :: term()) ::
              {:ok, new_state :: term()} | {:error, term()}

  @doc """
  Delete a key from storage.
  """
  @callback delete(key :: binary(), state :: term()) ::
              {:ok, new_state :: term()} | {:error, term()}

  @doc """
  Scan a range of keys.
  """
  @callback scan(start_key :: binary(), end_key :: binary(), opts :: keyword(), state :: term()) ::
              {:ok, Enumerable.t()} | {:error, term()}

  @doc """
  Flush any pending writes.
  """
  @callback flush(state :: term()) :: {:ok, new_state :: term()} | {:error, term()}

  @doc """
  Shutdown the storage engine.
  """
  @callback shutdown(state :: term()) :: :ok

  @optional_callbacks flush: 1
end

defmodule Store.Plugin.Protocol do
  @moduledoc """
  Behaviour for custom protocol plugins.

  Allows extending SpireDB with custom wire protocols
  (e.g., GraphQL, custom binary protocols, WebSocket).
  """

  @doc """
  Return available commands.
  Each command is {name, arity, description}.
  """
  @callback commands() :: [{String.t(), non_neg_integer(), String.t()}]

  @doc """
  Parse an incoming request into a command.
  """
  @callback parse(data :: binary(), state :: term()) ::
              {:ok, command :: term(), remaining :: binary(), new_state :: term()}
              | {:incomplete, new_state :: term()}
              | {:error, reason :: term()}

  @doc """
  Execute a parsed command.
  """
  @callback execute(command :: term(), state :: term()) ::
              {:ok, response :: term(), new_state :: term()}
              | {:error, reason :: term(), new_state :: term()}

  @doc """
  Encode a response for the wire.
  """
  @callback encode(response :: term()) :: binary()

  @doc """
  Handle connection open.
  """
  @callback on_connect(opts :: keyword()) :: {:ok, state :: term()} | {:error, term()}

  @doc """
  Handle connection close.
  """
  @callback on_disconnect(state :: term()) :: :ok

  @optional_callbacks on_connect: 1, on_disconnect: 1
end

defmodule Store.Plugin.Auth do
  @moduledoc """
  Behaviour for authentication/authorization plugins.

  Allows extending SpireDB with custom auth mechanisms
  (e.g., LDAP, OAuth, custom ACLs).
  """

  @type auth_result :: :allow | :deny | {:deny, reason :: String.t()}

  @doc """
  Authenticate a connection.
  Returns user identity on success.
  """
  @callback authenticate(credentials :: map()) ::
              {:ok, user :: map()} | {:error, reason :: term()}

  @doc """
  Authorize an operation.
  """
  @callback authorize(user :: map(), operation :: atom(), resource :: String.t()) ::
              auth_result()

  @doc """
  Check if user has a specific permission.
  """
  @callback has_permission?(user :: map(), permission :: atom()) :: boolean()

  @doc """
  Refresh user session/token.
  """
  @callback refresh(user :: map()) :: {:ok, new_user :: map()} | {:error, term()}

  @doc """
  Invalidate/logout a user session.
  """
  @callback invalidate(user :: map()) :: :ok

  @optional_callbacks refresh: 1, invalidate: 1, has_permission?: 2
end
