defmodule Hutch.Broadway.RabbitProducer do
  @moduledoc """
  A Broadway producer module for RabbitMQ integration with Hutch.

  This module provides a convenient way to create Broadway pipelines that consume
  messages from RabbitMQ queues managed by Hutch. It handles queue creation,
  connection setup, and message processing.

  ## Usage

  ```elixir
  defmodule MyApp.NotificationConsumer do
    use Hutch.Broadway.RabbitProducer,
      queue_manager: MyApp.QueueManager,
      exchange: "notifications",
      routing_key: "user_notifications",
      retry: true,
      worker_count: 5
  end
  ```

  Then you can start your Broadway pipeline, or add it to an application supervision tree:

  ```elixir
  MyApp.NotificationConsumer.start_link([])
  ```

  ## Options

  Required options:

    * `queue_manager` - The Hutch queue manager module
    * `exchange` - The exchange to bind the queue to
    * `routing_key` - The routing key for the queue

  Optional options:

    * `name` - The name of the Broadway pipeline (default: the module using this)
    * `prefix` - Queue name prefix (default: queue_manager.prefix())
    * `worker_count` - Number of Broadway workers (default: 2)
    * `prefetch_count` - Number of messages to prefetch (default: 20)
    * `processors` - Broadway processors configuration (default: [default: []])
    * `batchers` - Broadway batchers configuration (default: [])
    * `durable` - Whether the queue should be durable (default: true)
    * `retry` - Whether to enable retry mechanism (default: false)
    * `ttl` - Time-to-live for messages (default: queue_manager.default_ttl_ms())
    * `partitioned_by` - Function to partition messages by (default: nil)

  ## Message Processing

  By default, this module provides a `decode_payload/1` function that attempts to
  decode the message data as JSON. You can override this function in your module:

  ```elixir
  defmodule MyApp.NotificationConsumer do
    use Hutch.Broadway.RabbitProducer,
      queue_manager: MyApp.QueueManager,
      exchange: "notifications",
      routing_key: "user_notifications"

    def decode_payload(msg) do
      # Custom decoding logic
      msg
    end
  end
  ```
  """
  alias Broadway.Message

  require Logger

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      require Logger

      @queue_manager Keyword.fetch!(opts, :queue_manager)
      @batchers Keyword.get(opts, :batchers, [])
      @durable Keyword.get(opts, :durable, true)
      @exchange Keyword.fetch!(opts, :exchange)
      @name Keyword.get(opts, :name, __MODULE__)
      @prefetch_count Keyword.get(opts, :prefetch_count, 20)
      @prefix Keyword.get(opts, :prefix, @queue_manager.prefix())
      @processors Keyword.get(opts, :processors, default: [])
      @retry Keyword.get(opts, :retry, false)
      @routing_key Keyword.fetch!(opts, :routing_key)
      @ttl Keyword.get(opts, :ttl, @queue_manager.default_ttl_ms())
      @worker_count Keyword.get(opts, :worker_count, 2)

      use Broadway

      @doc """
      Starts the Broadway pipeline.

      This function:
      1. Creates the RabbitMQ queue if it doesn't exist
      2. Sets up the Broadway producer with the appropriate configuration
      3. Starts the Broadway supervision tree

      ## Parameters

        * `opts` - Options passed to Broadway.start_link/2 (usually empty)

      ## Returns

        * `{:ok, pid}` - The PID of the Broadway supervisor
        * `{:error, reason}` - If starting the pipeline fails
      """
      @spec start_link(term()) :: Supervisor.on_start()
      def start_link(_opts) do
        producer = [
          module:
            {BroadwayRabbitMQ.Producer,
             connection: @queue_manager.rabbit_url(),
             queue: "#{@prefix}.#{@routing_key}",
             qos: [prefetch_count: @prefetch_count],
             on_failure: :reject},
          concurrency: @worker_count
        ]

        Hutch.create_queue(@routing_key,
          exchange: @exchange,
          ttl: @ttl,
          durable: @durable,
          retry: @retry
        )

        Broadway.start_link(
          __MODULE__,
          with_partition_by(
            name: @name,
            producer: producer,
            processors: @processors,
            batchers: @batchers
          )
        )
      end

      @doc """
      Decodes the payload of a Broadway message.

      By default, this function attempts to decode the message data as JSON.
      If successful, it updates the message data with the decoded JSON.
      If decoding fails, it marks the message as failed.

      This function can be overridden in your module to provide custom decoding logic.

      ## Parameters

        * `msg` - A Broadway.Message struct

      ## Returns

        * A Broadway.Message struct with updated data or marked as failed
      """
      @spec decode_payload(Message.t()) :: Message.t()
      def decode_payload(msg) do
        case Jason.decode(msg.data) do
          {:ok, json} ->
            Message.update_data(msg, fn _ -> json end)

          {:error, _} ->
            err_msg = "Error decoding msg: #{msg.data}"
            Logger.error(err_msg)
            Message.failed(msg, err_msg)
        end
      end

      defoverridable decode_payload: 1

      defp with_partition_by(args) do
        partition_by = unquote(opts[:partitioned_by])
        if is_nil(partition_by), do: args, else: [{:partition_by, partition_by} | args]
      end
    end
  end
end
