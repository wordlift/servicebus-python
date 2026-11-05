"""Tests for wordlift_servicebus.publishers — publish, publish_topic, publish_batch, publish_scheduled."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from azure.servicebus import ServiceBusMessage, ServiceBusMessageBatch
from azure.servicebus._transport._pyamqp_transport import PyamqpTransport
from azure.servicebus.exceptions import MessageSizeExceededError

from wordlift_servicebus import publish, publish_batch, publish_scheduled, publish_topic
from wordlift_servicebus.publishers import _BATCH_SIZE_HEADROOM_BYTES

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

# Standard-tier link maximum, as reported by a default batch.
_LINK_MAX = 262_144
# _LINK_MAX - _BATCH_SIZE_HEADROOM_BYTES, spelled out on purpose.
_CAPPED_SIZE = 260_096


def _mock_batch(length: int = 0, add_side_effect: object = None) -> MagicMock:
    batch = MagicMock()
    batch.__len__ = MagicMock(return_value=length)
    batch.add_message = MagicMock(side_effect=add_side_effect)
    return batch


def _mock_batch_sender(*batches: MagicMock, link_max: int = _LINK_MAX) -> AsyncMock:
    """Sender whose first (default) batch reports ``link_max``, then yields ``batches``."""
    default_batch = MagicMock()
    default_batch.max_size_in_bytes = link_max
    sender = AsyncMock()
    sender.create_message_batch = AsyncMock(side_effect=[default_batch, *batches])
    return sender


class _RecordingBatch(ServiceBusMessageBatch):
    """A real SDK batch that also records the bodies it accepted, in order."""

    def __init__(self, max_size_in_bytes: int) -> None:
        super().__init__(max_size_in_bytes=max_size_in_bytes)
        self.bodies: list[str] = []

    def add_message(self, message: ServiceBusMessage) -> None:  # type: ignore[override]
        super().add_message(message)
        self.bodies.append(str(message))


def _real_batch_sender(link_max: int = _LINK_MAX) -> AsyncMock:
    """Real-batch sender; send_messages has no size check, like the SDK's."""

    async def create_message_batch(
        max_size_in_bytes: int | None = None,
    ) -> _RecordingBatch:
        if max_size_in_bytes and max_size_in_bytes > link_max:
            raise ValueError(f"Max message size: {max_size_in_bytes} is too large")
        return _RecordingBatch(max_size_in_bytes or link_max)

    sender = AsyncMock()
    sender.create_message_batch = AsyncMock(side_effect=create_message_batch)
    return sender


def _encoded_size(batch: ServiceBusMessageBatch) -> int:
    """Size of the batch as the sender encodes it, envelope included."""
    return PyamqpTransport.get_batch_message_encoded_size(batch._message)


def _sends(sender: AsyncMock) -> list[tuple[str, list[str]]]:
    """Each send as ("batch", bodies) or ("single", [body]), in send order."""
    result: list[tuple[str, list[str]]] = []
    for awaited in sender.send_messages.await_args_list:
        sent = awaited.args[0]
        if isinstance(sent, _RecordingBatch):
            result.append(("batch", list(sent.bodies)))
        else:
            result.append(("single", [str(sent)]))
    return result


@pytest.mark.asyncio
async def test_publish_batch_sends_all_in_one_batch() -> None:
    batch = _mock_batch(length=2)
    sender = _mock_batch_sender(batch)

    await publish_batch(
        _fake_client(sender), "q", [_serializable('{"a":1}'), _serializable('{"b":2}')]
    )

    assert batch.add_message.call_count == 2
    sender.send_messages.assert_awaited_once_with(batch)


@pytest.mark.asyncio
async def test_publish_batch_flushes_on_size_exceeded() -> None:
    batch1 = _mock_batch(length=1, add_side_effect=[None, MessageSizeExceededError()])
    batch2 = _mock_batch(length=1)
    sender = _mock_batch_sender(batch1, batch2)

    await publish_batch(
        _fake_client(sender), "q", [_serializable('{"a":1}'), _serializable('{"b":2}')]
    )

    assert sender.send_messages.await_args_list == [call(batch1), call(batch2)]


@pytest.mark.asyncio
async def test_publish_batch_empty_list_sends_nothing() -> None:
    sender = _mock_batch_sender(_mock_batch(length=0))

    await publish_batch(_fake_client(sender), "q", [])

    sender.send_messages.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_batch_caps_batches_below_link_max() -> None:
    assert _CAPPED_SIZE == _LINK_MAX - _BATCH_SIZE_HEADROOM_BYTES
    sender = _mock_batch_sender(_mock_batch(length=1))

    await publish_batch(_fake_client(sender), "q", [_serializable('{"a":1}')])

    assert sender.create_message_batch.await_args_list == [
        call(),
        call(max_size_in_bytes=_CAPPED_SIZE),
    ]


@pytest.mark.asyncio
async def test_publish_batch_flush_creates_capped_batch() -> None:
    batch1 = _mock_batch(length=1, add_side_effect=[None, MessageSizeExceededError()])
    batch2 = _mock_batch(length=1)
    sender = _mock_batch_sender(batch1, batch2)

    await publish_batch(
        _fake_client(sender), "q", [_serializable('{"a":1}'), _serializable('{"b":2}')]
    )

    assert sender.create_message_batch.await_args_list == [
        call(),
        call(max_size_in_bytes=_CAPPED_SIZE),
        call(max_size_in_bytes=_CAPPED_SIZE),
    ]


@pytest.mark.asyncio
async def test_publish_batch_sends_message_too_big_for_capped_batch_alone() -> None:
    # Fits the 262,144-byte link on its own, but not a 260,096-byte capped batch.
    big = "x" * 261_000
    sender = _real_batch_sender()

    await publish_batch(
        _fake_client(sender),
        "q",
        [_serializable(b) for b in ["a", "b", big, "c", "d"]],
    )

    assert _sends(sender) == [
        ("batch", ["a", "b"]),
        ("single", [big]),
        ("batch", ["c", "d"]),
    ]


@pytest.mark.asyncio
async def test_publish_batch_sends_leading_oversized_message_without_empty_batch() -> (
    None
):
    big = "x" * 261_000
    sender = _real_batch_sender()

    await publish_batch(
        _fake_client(sender), "q", [_serializable(big), _serializable("a")]
    )

    assert _sends(sender) == [("single", [big]), ("batch", ["a"])]


@pytest.mark.asyncio
async def test_publish_batch_message_too_big_for_link_raises() -> None:
    sender = _real_batch_sender()

    with pytest.raises(MessageSizeExceededError):
        await publish_batch(
            _fake_client(sender),
            "q",
            [_serializable("a"), _serializable("x" * 300_000)],
        )

    # Earlier messages were flushed; the oversized one never reached the sender.
    assert _sends(sender) == [("batch", ["a"])]


@pytest.mark.asyncio
async def test_publish_batch_near_full_batch_encodes_within_link_limit() -> None:
    """Regression (WOR-1604): a near-full batch must encode within the link limit."""
    # 9 messages, as in production, filling an uncapped batch to within 100 bytes.
    bodies = [f"{i}" * 29_000 for i in range(8)]
    probe = ServiceBusMessageBatch(max_size_in_bytes=_LINK_MAX)
    for body in bodies:
        probe.add_message(ServiceBusMessage(body))
    before_last = probe.size_in_bytes
    probe.add_message(ServiceBusMessage("z" * 1_000))
    per_message_overhead = probe.size_in_bytes - before_last - 1_000
    bodies.append("z" * (_LINK_MAX - before_last - per_message_overhead - 50))

    uncapped = ServiceBusMessageBatch(max_size_in_bytes=_LINK_MAX)
    for body in bodies:
        uncapped.add_message(ServiceBusMessage(body))
    assert _LINK_MAX - 100 <= uncapped.size_in_bytes <= _LINK_MAX

    sender = _real_batch_sender()
    await publish_batch(_fake_client(sender), "q", [_serializable(b) for b in bodies])

    sent_batches = [a.args[0] for a in sender.send_messages.await_args_list]
    assert all(isinstance(b, _RecordingBatch) for b in sent_batches)
    assert all(_encoded_size(b) <= _LINK_MAX for b in sent_batches)
    assert [body for _, batch in _sends(sender) for body in batch] == bodies
