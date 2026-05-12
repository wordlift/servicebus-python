# wordlift-servicebus

Reusable Azure Service Bus consumers, publishers, and receiver factories for WordLift Python services.

## Installation

Releases are published as wheel assets on [GitHub Releases](https://github.com/wordlift/wordlift-servicebus-python/releases). Add the dependency via a git tag reference:

```bash
uv add "wordlift-servicebus @ git+https://github.com/wordlift/wordlift-servicebus-python@0.1.0"
```

Or pin the source in `pyproject.toml` while keeping the version constraint separate:

```toml
[project]
dependencies = ["wordlift-servicebus>=0.1.0"]

[tool.uv.sources]
wordlift-servicebus = { git = "https://github.com/wordlift/wordlift-servicebus-python", tag = "0.1.0" }
```

## Public API

### Consumers

Three consumer patterns are provided — all accept a `receiver_factory` callable that returns an async context manager yielding a `ServiceBusReceiver`:

```python
from wordlift_servicebus import (
    SequentialConsumer,  # one message at a time, async-for iterator
    BatchConsumer,       # receive_messages batches with idle logging
    ConcurrentConsumer,  # slot-based back-pressure, reconnects on empty receiver
)
```

| Consumer | Handler signature | Use when |
|---|---|---|
| `SequentialConsumer` | `async def handler(msg: M) -> None` | order matters, low throughput |
| `BatchConsumer` | `async def handler(batch: list[M]) -> None` | bulk DB writes, high throughput |
| `ConcurrentConsumer` | `async def handler(msg: M) -> None` | I/O-bound fan-out, high throughput |

### Publishers

```python
from wordlift_servicebus import publish, publish_batch, publish_scheduled, publish_topic
```

| Function | Description |
|---|---|
| `publish(client, queue, msg)` | Send a single message to a queue |
| `publish_topic(client, topic, msg)` | Send a single message to a topic |
| `publish_batch(client, queue, msgs)` | Send messages in batches, auto-flushing on size limit |
| `publish_scheduled(client, queue, msg, delay_seconds)` | Enqueue a message with a future delivery time |

### Receiver factories

```python
from wordlift_servicebus import queue_receiver_factory, subscription_receiver_factory
```

These helpers build the `receiver_factory` callable expected by all consumers. They default `max_wait_time=5.0` (required by `SequentialConsumer`'s async iterator):

```python
factory = queue_receiver_factory(client, "my-queue")
factory = subscription_receiver_factory(client, "my-topic", "my-subscription")
```

Extra kwargs are forwarded to the underlying `ServiceBusClient` call (e.g. `prefetch_count`).

## Quick example

```python
import asyncio
from azure.servicebus.aio import ServiceBusClient
from wordlift_servicebus import (
    ConcurrentConsumer,
    queue_receiver_factory,
    publish,
)

class MyMessage:
    @classmethod
    def from_payload(cls, data): ...
    def model_dump_json(self, *, by_alias=False): ...

async def handle(msg: MyMessage) -> None:
    print(f"Processing {msg}")

async def main() -> None:
    stopped = asyncio.Event()
    async with ServiceBusClient.from_connection_string("...") as client:
        # publish
        await publish(client, "my-queue", MyMessage())

        # consume
        factory = queue_receiver_factory(client, "my-queue")
        consumer = ConcurrentConsumer(stopped)
        await consumer.consume(factory, "queue=my-queue", MyMessage, handle, 10, 300.0)

asyncio.run(main())
```

## Protocols

Messages must implement the structural protocols in `wordlift_servicebus.protocols`:

- `ParseableMessage` — requires `from_payload(cls, data: dict) -> Self`
- `SerializableMessage` — requires `model_dump_json(*, by_alias: bool) -> str`
- `BusMessage` — both combined

## Development

```bash
uv sync --dev --frozen
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -q --cov=src
```
