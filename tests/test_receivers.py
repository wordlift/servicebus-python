"""Tests for wordlift_servicebus.receivers — queue_receiver_factory, subscription_receiver_factory."""

from __future__ import annotations

from unittest.mock import MagicMock

from wordlift_servicebus import queue_receiver_factory, subscription_receiver_factory


def test_queue_receiver_factory_defaults_max_wait_time() -> None:
    client = MagicMock()
    queue_receiver_factory(client, "my-queue")()
    client.get_queue_receiver.assert_called_once_with("my-queue", max_wait_time=5.0)


def test_queue_receiver_factory_passes_custom_kwargs() -> None:
    client = MagicMock()
    queue_receiver_factory(client, "my-queue", max_wait_time=10.0, prefetch_count=20)()
    client.get_queue_receiver.assert_called_once_with(
        "my-queue", max_wait_time=10.0, prefetch_count=20
    )


def test_subscription_receiver_factory_defaults_max_wait_time() -> None:
    client = MagicMock()
    subscription_receiver_factory(client, "my-topic", "my-sub")()
    client.get_subscription_receiver.assert_called_once_with(
        "my-topic", "my-sub", max_wait_time=5.0
    )


def test_subscription_receiver_factory_passes_custom_max_wait_time() -> None:
    client = MagicMock()
    subscription_receiver_factory(client, "my-topic", "my-sub", max_wait_time=30.0)()
    client.get_subscription_receiver.assert_called_once_with(
        "my-topic", "my-sub", max_wait_time=30.0
    )
