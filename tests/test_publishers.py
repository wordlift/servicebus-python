"""Tests for wordlift_servicebus.publishers — publish, publish_topic, publish_batch, publish_scheduled."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from azure.servicebus.exceptions import MessageSizeExceededError

from wordlift_servicebus import publish, publish_batch, publish_scheduled, publish_topic

from .helpers import _fake_client, _fake_sender_ctx, _serializable


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_sends_single_message() -> None:
    sender = AsyncMock()
    client = _fake_client(sender)

    await publish(client, "my-queue", _serializable('{"a":1}'))

    client.get_queue_sender.assert_called_once_with("my-queue")
    sender.send_messages.assert_awaited_once()


# ---------------------------------------------------------------------------
# publish_topic
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_topic_sends_message() -> None:
    sender = AsyncMock()
    client = MagicMock()
    client.get_topic_sender.return_value = _fake_sender_ctx(sender)

    await publish_topic(client, "my-topic", _serializable('{"a":1}'))

    client.get_topic_sender.assert_called_once_with("my-topic")
    sender.send_messages.assert_awaited_once()


# ---------------------------------------------------------------------------
# publish_scheduled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_scheduled_delays_by_requested_seconds() -> None:
    sender = AsyncMock()
    client = _fake_client(sender)

    before = datetime.now(tz=timezone.utc)
    await publish_scheduled(
        client, "my-queue", _serializable('{"a":1}'), delay_seconds=10.0
    )
    after = datetime.now(tz=timezone.utc)

    sent = sender.send_messages.call_args[0][0]
    assert sent.scheduled_enqueue_time_utc >= before + timedelta(seconds=10.0)
    assert sent.scheduled_enqueue_time_utc <= after + timedelta(seconds=10.0)


# ---------------------------------------------------------------------------
# publish_batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_batch_sends_all_in_one_batch() -> None:
    batch = MagicMock()
    batch.__len__ = MagicMock(return_value=2)
    sender = AsyncMock()
    sender.create_message_batch = AsyncMock(return_value=batch)

    await publish_batch(
        _fake_client(sender), "q", [_serializable('{"a":1}'), _serializable('{"b":2}')]
    )

    assert batch.add_message.call_count == 2
    sender.send_messages.assert_awaited_once_with(batch)


@pytest.mark.asyncio
async def test_publish_batch_flushes_on_size_exceeded() -> None:
    batch1 = MagicMock()
    batch1.__len__ = MagicMock(return_value=1)
    batch1.add_message = MagicMock(side_effect=[None, MessageSizeExceededError()])

    batch2 = MagicMock()
    batch2.__len__ = MagicMock(return_value=1)

    sender = AsyncMock()
    sender.create_message_batch = AsyncMock(side_effect=[batch1, batch2])

    await publish_batch(
        _fake_client(sender), "q", [_serializable('{"a":1}'), _serializable('{"b":2}')]
    )

    assert sender.send_messages.await_count == 2
    sender.send_messages.assert_any_await(batch1)
    sender.send_messages.assert_any_await(batch2)


@pytest.mark.asyncio
async def test_publish_batch_empty_list_sends_nothing() -> None:
    batch = MagicMock()
    batch.__len__ = MagicMock(return_value=0)
    sender = AsyncMock()
    sender.create_message_batch = AsyncMock(return_value=batch)

    await publish_batch(_fake_client(sender), "q", [])

    sender.send_messages.assert_not_awaited()
