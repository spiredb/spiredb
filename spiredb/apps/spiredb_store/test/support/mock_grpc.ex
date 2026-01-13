defmodule Store.Test.MockGRPC do
  @moduledoc false
  def send_reply(stream, response) do
    # Simulate sending reply by sending message to current process (test process)
    send(self(), {:grpc_reply, response})
    stream
  end
end
