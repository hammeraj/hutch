defmodule Hutch do
  @moduledoc """
  Helper for creating queues on consumer init.
  This is functionally idempotent, it will not create new queues if the same
  configuration is passed through, but it will fail if the config has changed
  and a queue with that name already exists.

  This also handles creating the exchanges if they don't already exist. This assumes
  a policy of "topic" for all exchanges as this will handle most of the use cases.

  ## Using Options

  Required options for `use Hutch`:
    * `:rabbit_url` - The RabbitMQ connection URL
    * `:prefix` - Queue name prefix

  Optional options for `use Hutch`:
    * `:default_ttl_ms` - Default TTL for messages (default: 2 hours)
  """

  @callback rabbit_url() :: String.t()
  @callback prefix() :: String.t()
  @callback default_ttl_ms() :: integer()

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Hutch
      @rabbit_url Keyword.fetch!(opts, :rabbit_url)
      @prefix Keyword.fetch!(opts, :prefix)
      @hutch_default_ttl_ms :timer.hours(2)
      @default_ttl_ms Keyword.get(opts, :default_ttl_ms, @hutch_default_ttl_ms)

      def rabbit_url do
        @rabbit_url
      end

      def prefix do
        @prefix
      end

      def default_ttl_ms do
        @default_ttl_ms
      end

      @doc """
      Creates a queue with the specified name and options.

      ## Options

        * `ttl` - Time-to-live for messages in milliseconds (default: `120000`)
        * `durable` - Whether the queue should survive broker restarts (default: `true`)
        * `retry` - Whether to enable retry mechanism (default: `false`)
        * `exchange` - Optional exchange to bind the queue to
        * `prefix` - Override the default prefix for this queue
        * `single_active_consumer_opts` - Options for single active consumer. Defaults to `[enabled: false, count: 1]`
          * `enabled` - When true, ensures only one consumer is active at a time
          * `count` - Number of active consumers (currently unused, reserved for future use)

      See [RabbitMQ Single Active Consumer](https://www.rabbitmq.com/consumers.html#single-active-consumer)
      for more information about the single active consumer feature.

      ## Examples

          iex> create_queue("my_queue", ttl: 60_000, retry: true)
          :ok

          iex> create_queue("single_consumer_queue",
          ...>   single_active_consumer_opts: [enabled: true])
          :ok
      """
      @spec create_queue(String.t(), Keyword.t()) :: :ok | {:error, any()}
      def create_queue(queue_name, opts) do
        with_channel(fn channel ->
          create_queue(channel, queue_name, opts)
        end)
      end

      @doc false
      @spec create_queue(AMQP.Channel.t(), String.t(), Keyword.t()) :: :ok | {:error, any()}
      def create_queue(channel, queue_name, opts) do
        ttl = Keyword.get(opts, :ttl, default_ttl_ms())
        durable = Keyword.get(opts, :durable, true)
        retry = Keyword.get(opts, :retry, false)
        exchange = Keyword.get(opts, :exchange)
        queue_type = Keyword.get(opts, :queue_type, :classic)
        prefix = Keyword.get(opts, :prefix, @prefix) <> "."
        single_active_consumer_opts = Keyword.get(opts, :single_active_consumer_opts, [enabled: false, count: 1])
        fqn = prefix <> queue_name
        dlq = fqn <> ".dlq"

        if retry do
          declare_retry_queue(channel, fqn, dlq, ttl, durable)
        else
          declare_expire_queue(channel, dlq, ttl, durable)
        end

        declare_classic_queue(channel, fqn, dlq, durable, single_active_consumer_opts)

        if exchange do
          AMQP.Exchange.declare(channel, exchange, :topic)
          AMQP.Queue.bind(channel, fqn, exchange, routing_key: queue_name)
        end
      end

      defp with_channel(block) do
        {:ok, connection} = AMQP.Connection.open(rabbit_url())
        {:ok, channel} = AMQP.Channel.open(connection)
        block.(channel)
        AMQP.Connection.close(connection)
      end

      defp declare_expire_queue(channel, dead_letter, expiry, durable) do
        AMQP.Queue.declare(channel, dead_letter,
          durable: durable,
          arguments: [
            {"x-message-ttl", :signedint, expiry}
          ]
        )
      end

      defp declare_retry_queue(channel, queue_name, dead_letter, expiry, durable) do
        AMQP.Queue.declare(channel, dead_letter,
          durable: durable,
          arguments: [
            {"x-message-ttl", :signedint, expiry},
            {"x-dead-letter-exchange", :longstr, ""},
            {"x-dead-letter-routing-key", :longstr, queue_name}
          ]
        )
      end

      defp declare_classic_queue(channel, queue_name, dead_letter, durable, single_active_consumer_opts) do
        arguments = [
          {"x-dead-letter-exchange", :longstr, ""},
          {"x-dead-letter-routing-key", :longstr, dead_letter}
        ]

        arguments = if Keyword.get(single_active_consumer_opts, :enabled, false) do
          [{"x-single-active-consumer", :bool, true} | arguments]
        else
          arguments
        end

        AMQP.Queue.declare(channel, queue_name,
          durable: durable,
          arguments: arguments
        )
      end
    end
  end
end
