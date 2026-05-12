# wordlift-servicebus

Reusable Azure Service Bus consumers, publishers, and receiver factories for WordLift Python services.

## Installation

The package is published to [GitHub Packages](https://github.com/orgs/wordlift/packages). Add the registry to your `uv` configuration:

```toml
# pyproject.toml
[[tool.uv.index]]
name = "wordlift-github"
url = "https://pypi.pkg.github.com/wordlift/"
```

Then add the dependency:

```bash
uv add wordlift-servicebus
```

Authenticating against GitHub Packages requires a personal access token with `read:packages` scope:

```bash
export UV_INDEX_WORDLIFT_GITHUB_USERNAME=<github-username>
export UV_INDEX_WORDLIFT_GITHUB_PASSWORD=<github-pat>
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
