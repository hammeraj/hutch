# Hutch

Hutch is an Elixir library that simplifies RabbitMQ queue management and message processing using Broadway. It provides helper functions for creating queues with dead-letter queues (DLQ) support, retry mechanisms, and message TTL configuration.

## Features

- Declarative queue setup with automatic DLQ creation
- Configurable message TTL and retry mechanisms
- Broadway integration for efficient message processing
- Topic exchange support
- Prefix-based queue naming for better organization
- Single active consumer support for exclusive processing

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `hutch` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:hutch, "~> 0.1.0"}
  ]
end
```

## Basic Usage

First, include Hutch in your queue manager module:

```elixir
defmodule MyApp.QueueManager do
  use Hutch,
    rabbit_url: "amqp://guest:guest@localhost:5672",
    prefix: "myapp",
    default_ttl_ms: :timer.hours(1)
end
```

Then create queues as needed (consumers will pass through args and self create queues on start_link):

```elixir
# Create a basic queue
MyApp.QueueManager.create_queue("notifications")

# Create a queue with retry enabled, which will dead letter failed messages and move them back to the queue on expiration
MyApp.QueueManager.create_queue("orders", retry: true)

# Create a queue bound to an exchange
MyApp.QueueManager.create_queue("user_events", 
  exchange: "users", 
  ttl: 3_600_000)
```

## Queue Naming

Hutch automatically prefixes queue names with the configured prefix:

```elixir
# With prefix "myapp", this creates "myapp.notifications"
MyApp.QueueManager.create_queue("notifications")
```

## Dead Letter Queues

Hutch automatically creates a dead letter queue (DLQ) for each queue:

```elixir
# Creates "myapp.notifications" and "myapp.notifications.dlq"
MyApp.QueueManager.create_queue("notifications")
```

## Retry Mechanism

When the retry option is enabled, failed messages are routed to the DLQ
with a TTL, after which they are automatically requeued to the original queue:

```elixir
# Enable retry for automatic reprocessing of failed messages
MyApp.QueueManager.create_queue("orders", retry: true)
```

## Single Active Consumer

Hutch supports RabbitMQ's Single Active Consumer feature, which ensures only one consumer processes messages at a time:

```elixir
# Create a queue with single active consumer enabled
MyApp.QueueManager.create_queue("exclusive_processing", 
  single_active_consumer_opts: [enabled: true])
```

This is useful for operations that require exclusive access or strict ordering of message processing.

## Queue Creation Options

When creating queues with `create_queue/2`, the following options are available:

* `ttl` - Time-to-live for messages in milliseconds (default: configured default_ttl_ms)
* `durable` - Whether the queue should survive broker restarts (default: `true`)
* `retry` - Whether to enable retry mechanism (default: `false`)
* `exchange` - Optional exchange to bind the queue to
* `prefix` - Override the default prefix for this queue
* `single_active_consumer_opts` - Options for single active consumer. Defaults to `[enabled: false, count: 1]`
  * `enabled` - When true, ensures only one consumer is active at a time
  * `count` - Number of active consumers (currently unused, reserved for future use)

See [RabbitMQ Single Active Consumer](https://www.rabbitmq.com/consumers.html#single-active-consumer) for more information about the single active consumer feature.

## Broadway Integration

Hutch provides a Broadway producer module for easy integration with Broadway:

```elixir
defmodule MyApp.NotificationProcessor do
  use Hutch.Broadway.RabbitProducer,
    queue_manager: MyApp.QueueManager,
    exchange: "notifications",
    routing_key: "user_notifications",
    retry: true,
    worker_count: 5
end
```
