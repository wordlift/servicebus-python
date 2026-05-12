from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, Callable

from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver

_DEFAULT_RECEIVER_MAX_WAIT_S = 5.0


def queue_receiver_factory(
    client: ServiceBusClient,
    queue_name: str,
    max_wait_time: float = _DEFAULT_RECEIVER_MAX_WAIT_S,
    **kwargs: Any,
) -> Callable[[], AbstractAsyncContextManager[ServiceBusReceiver]]:
    """Return a factory that opens a queue receiver with a safe default max_wait_time.

    The receiver-level max_wait_time is required by SequentialConsumer (which uses
    the async iterator); without it the iterator blocks indefinitely.
    """

    def _factory() -> AbstractAsyncContextManager[ServiceBusReceiver]:
        return client.get_queue_receiver(
            queue_name, max_wait_time=max_wait_time, **kwargs
        )

    return _factory


def subscription_receiver_factory(
    client: ServiceBusClient,
    topic_name: str,
    subscription_name: str,
    max_wait_time: float = _DEFAULT_RECEIVER_MAX_WAIT_S,
    **kwargs: Any,
) -> Callable[[], AbstractAsyncContextManager[ServiceBusReceiver]]:
    """Return a factory that opens a topic-subscription receiver with a safe default max_wait_time."""

    def _factory() -> AbstractAsyncContextManager[ServiceBusReceiver]:
        return client.get_subscription_receiver(
            topic_name, subscription_name, max_wait_time=max_wait_time, **kwargs
        )

    return _factory
