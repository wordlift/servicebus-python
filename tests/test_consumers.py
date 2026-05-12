"""Tests for wordlift_servicebus.consumers — BatchConsumer, SequentialConsumer, ConcurrentConsumer."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wordlift_servicebus import BatchConsumer, ConcurrentConsumer, SequentialConsumer

from .helpers import (
    _LOCK_RENEWAL_MAX_S,
    _AsyncCtx,
    _FakeBatchReceiver,
    _FakeReceiver,
    _TestMessage,
    _payload,
)


async def _run_batch(
    receiver: _FakeBatchReceiver,
    handler: AsyncMock,
) -> None:
    stopped = asyncio.Event()
    consumer = BatchConsumer(stopped)
    call_count = 0

    def _is_set() -> bool:
        nonlocal call_count
        call_count += 1
        return not receiver._batches and call_count > 1

    stopped.is_set = _is_set  # type: ignore[method-assign]
    await consumer._run_loop(
        receiver,
        MagicMock(),
        _TestMessage,
        handler,
        "queue=test",
        50,
        5.0,
        _LOCK_RENEWAL_MAX_S,
    )


# ---------------------------------------------------------------------------
# BatchConsumer._run_loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_loop_registers_lock_renewer_per_message() -> None:
    stopped = asyncio.Event()
    consumer = BatchConsumer(stopped)
    renewer = MagicMock()
    receiver = _FakeBatchReceiver([[_payload(id="r1"), _payload(id="r2")]])

    call_count = 0

    def _is_set() -> bool:
        nonlocal call_count
        call_count += 1
        return not receiver._batches and call_count > 1

    stopped.is_set = _is_set  # type: ignore[method-assign]
    await consumer._run_loop(
        receiver,
        renewer,
        _TestMessage,
        AsyncMock(),
        "queue=test",
        50,
        5.0,
        _LOCK_RENEWAL_MAX_S,
    )

    assert renewer.register.call_count == 2


@pytest.mark.asyncio
async def test_batch_loop_happy_path_completes_all_messages() -> None:
    handler = AsyncMock()
    receiver = _FakeBatchReceiver([[_payload(id="r1"), _payload(id="r2")]])

    await _run_batch(receiver, handler)

    handler.assert_awaited_once()
    batch: list[_TestMessage] = handler.call_args[0][0]
    assert len(batch) == 2
    assert receiver.complete_message.call_count == 2
    receiver.dead_letter_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_loop_parse_error_dead_letters_bad_message_only() -> None:
    handler = AsyncMock()
    bad = json.dumps({"_class": "WrongClass", "_version": 1})
    good = _payload(id="r-good")
    receiver = _FakeBatchReceiver([[bad, good]])

    await _run_batch(receiver, handler)

    receiver.dead_letter_message.assert_awaited_once()
    receiver.complete_message.assert_awaited_once()
    handler.assert_awaited_once()
    batch: list[_TestMessage] = handler.call_args[0][0]
    assert len(batch) == 1
    assert batch[0].id == "r-good"


@pytest.mark.asyncio
async def test_batch_loop_all_parse_errors_skips_handler() -> None:
    handler = AsyncMock()
    bad = json.dumps({"_class": "WrongClass", "_version": 1})
    receiver = _FakeBatchReceiver([[bad, bad]])

    await _run_batch(receiver, handler)

    assert receiver.dead_letter_message.call_count == 2
    handler.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_loop_handler_exception_dead_letters_all() -> None:
    handler = AsyncMock(side_effect=RuntimeError("db down"))
    receiver = _FakeBatchReceiver([[_payload(id="r1"), _payload(id="r2")]])

    await _run_batch(receiver, handler)

    assert receiver.dead_letter_message.call_count == 2
    receiver.complete_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_loop_logs_idle_after_30s_silence() -> None:
    stopped = asyncio.Event()
    consumer = BatchConsumer(stopped)
    receiver = _FakeBatchReceiver([])  # only the trailing empty batch

    call_count = 0

    def _is_set() -> bool:
        nonlocal call_count
        call_count += 1
        return not receiver._batches and call_count > 1

    stopped.is_set = _is_set  # type: ignore[method-assign]

    start = 1000.0
    with (
        patch("wordlift_servicebus.consumers.time") as mock_time,
        patch("wordlift_servicebus.consumers.logger") as mock_logger,
    ):
        mock_time.monotonic.side_effect = [start, start + 31.0]
        await consumer._run_loop(
            receiver,
            MagicMock(),
            _TestMessage,
            AsyncMock(),
            "queue=test",
            50,
            5.0,
            _LOCK_RENEWAL_MAX_S,
        )

    idle_calls = [
        c for c in mock_logger.debug.call_args_list if "No messages" in str(c)
    ]
    assert len(idle_calls) == 1


# ---------------------------------------------------------------------------
# SequentialConsumer.consume — factory wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sequential_consumer_processes_messages_via_factory() -> None:
    stopped = asyncio.Event()
    consumer = SequentialConsumer(stopped)
    processed: list[str] = []

    async def handler(msg: _TestMessage) -> None:
        processed.append(msg.id)

    receiver = _FakeReceiver([_payload(id="s1"), _payload(id="s2")])

    mock_renewer = MagicMock()
    mock_renewer.__aenter__ = AsyncMock(return_value=mock_renewer)
    mock_renewer.__aexit__ = AsyncMock(return_value=False)
    mock_renewer.register = MagicMock()

    factory = MagicMock(return_value=_AsyncCtx(receiver))

    with patch(
        "wordlift_servicebus.consumers.AutoLockRenewer", return_value=mock_renewer
    ):
        await consumer.consume(
            factory,
            "test-source",
            _TestMessage,
            AsyncMock(side_effect=handler),
            _LOCK_RENEWAL_MAX_S,
        )

    factory.assert_called_once()
    assert processed == ["s1", "s2"]
    assert receiver.complete_message.call_count == 2


# ---------------------------------------------------------------------------
# BatchConsumer.consume — factory wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_consumer_uses_receiver_factory() -> None:
    stopped = asyncio.Event()
    consumer = BatchConsumer(stopped)
    handler = AsyncMock()
    receiver = _FakeBatchReceiver([[_payload()]])

    mock_renewer = MagicMock()
    mock_renewer.__aenter__ = AsyncMock(return_value=mock_renewer)
    mock_renewer.__aexit__ = AsyncMock(return_value=False)
    mock_renewer.register = MagicMock()

    factory = MagicMock(return_value=_AsyncCtx(receiver))

    call_count = 0

    def _is_set() -> bool:
        nonlocal call_count
        call_count += 1
        return not receiver._batches and call_count > 1

    stopped.is_set = _is_set  # type: ignore[method-assign]

    with patch(
        "wordlift_servicebus.consumers.AutoLockRenewer", return_value=mock_renewer
    ):
        await consumer.consume(
            factory, "test-source", _TestMessage, handler, 50, 5.0, _LOCK_RENEWAL_MAX_S
        )

    factory.assert_called_once()
    handler.assert_awaited_once()


# ---------------------------------------------------------------------------
# ConcurrentConsumer.consume
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_consumer_processes_all_messages() -> None:
    stopped = asyncio.Event()
    consumer = ConcurrentConsumer(stopped)
    processed: list[str] = []

    async def handler(msg: _TestMessage) -> None:
        processed.append(msg.id)

    receiver = _FakeBatchReceiver([[_payload(id="c1"), _payload(id="c2")]])

    mock_renewer = MagicMock()
    mock_renewer.__aenter__ = AsyncMock(return_value=mock_renewer)
    mock_renewer.__aexit__ = AsyncMock(return_value=False)
    mock_renewer.register = MagicMock()

    factory = MagicMock(return_value=_AsyncCtx(receiver))
    stopped.is_set = lambda: len(receiver._batches) == 0  # type: ignore[method-assign]

    with patch(
        "wordlift_servicebus.consumers.AutoLockRenewer", return_value=mock_renewer
    ):
        await consumer.consume(
            factory,
            "queue=test",
            _TestMessage,
            AsyncMock(side_effect=handler),
            10,
            _LOCK_RENEWAL_MAX_S,
        )

    assert set(processed) == {"c1", "c2"}
    assert receiver.complete_message.call_count == 2


@pytest.mark.asyncio
async def test_concurrent_consumer_respects_max_concurrent() -> None:
    stopped = asyncio.Event()
    consumer = ConcurrentConsumer(stopped)
    receiver = _FakeBatchReceiver([[_payload(id=str(i)) for i in range(5)]])

    mock_renewer = MagicMock()
    mock_renewer.__aenter__ = AsyncMock(return_value=mock_renewer)
    mock_renewer.__aexit__ = AsyncMock(return_value=False)
    mock_renewer.register = MagicMock()

    factory = MagicMock(return_value=_AsyncCtx(receiver))
    stopped.is_set = lambda: len(receiver._batches) == 0  # type: ignore[method-assign]

    with patch(
        "wordlift_servicebus.consumers.AutoLockRenewer", return_value=mock_renewer
    ):
        await consumer.consume(
            factory, "queue=test", _TestMessage, AsyncMock(), 3, _LOCK_RENEWAL_MAX_S
        )

    assert receiver._receive_calls[0]["max_message_count"] <= 3
