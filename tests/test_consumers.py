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


@pytest.mark.asyncio
async def test_concurrent_consumer_continues_on_empty_batch() -> None:
    """An empty batch must not disconnect; the consumer keeps polling on the same connection."""
    stopped = asyncio.Event()
    consumer = ConcurrentConsumer(stopped)
    processed: list[str] = []

    async def handler(msg: _TestMessage) -> None:
        processed.append(msg.id)

    # leading empty batch, then a real message
    receiver = _FakeBatchReceiver([[], [_payload(id="c1")]])

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

    assert processed == ["c1"]
    assert receiver.complete_message.call_count == 1
    factory.assert_called_once()  # no reconnect on empty batch


@pytest.mark.asyncio
async def test_concurrent_consumer_prunes_completed_tasks_on_idle_poll() -> None:
    """Done task refs must be removed from the internal list on each idle poll.

    Without the prune, the list grows unboundedly across idle cycles (memory leak).
    The receiver here waits for all in-flight tasks to finish before returning the
    idle batch, which guarantees every task is .done() when the prune line runs.
    """
    stopped = asyncio.Event()
    consumer = ConcurrentConsumer(stopped)

    created_tasks: list[asyncio.Task] = []
    _orig_create_task = asyncio.create_task

    def _track(coro, **kw):  # type: ignore[no-untyped-def]
        t = _orig_create_task(coro, **kw)
        created_tasks.append(t)
        return t

    class _BarrierReceiver(_FakeBatchReceiver):
        """Waits for all previously-created tasks to finish before each receive."""

        async def receive_messages(
            self, max_message_count: int, max_wait_time: float
        ) -> list[str]:
            if created_tasks:
                await asyncio.gather(*created_tasks, return_exceptions=True)
            return await super().receive_messages(max_message_count, max_wait_time)

    # one real batch of 2, then an idle batch, then stop
    receiver = _BarrierReceiver([[_payload(id="p1"), _payload(id="p2")], []])

    mock_renewer = MagicMock()
    mock_renewer.__aenter__ = AsyncMock(return_value=mock_renewer)
    mock_renewer.__aexit__ = AsyncMock(return_value=False)
    mock_renewer.register = MagicMock()

    factory = MagicMock(return_value=_AsyncCtx(receiver))
    stopped.is_set = lambda: len(receiver._batches) == 0  # type: ignore[method-assign]

    with patch(
        "wordlift_servicebus.consumers.AutoLockRenewer", return_value=mock_renewer
    ):
        with patch("asyncio.create_task", side_effect=_track):
            await consumer.consume(
                factory,
                "queue=test",
                _TestMessage,
                AsyncMock(),
                10,
                _LOCK_RENEWAL_MAX_S,
            )

    assert len(created_tasks) == 2, "one task per message"
    assert all(t.done() for t in created_tasks), "all tasks must have completed"
    assert receiver.complete_message.call_count == 2, "all messages must be acked"
    factory.assert_called_once()  # no reconnect on idle poll


@pytest.mark.asyncio
async def test_concurrent_consumer_prunes_renewer_futures_on_idle_poll() -> None:
    """Done AutoLockRenewer futures must be removed from _futures on each idle poll.

    Each register() call appends a Future to renewer._futures. Without pruning,
    that list grows unboundedly across idle cycles even after messages are settled.
    """
    stopped = asyncio.Event()
    consumer = ConcurrentConsumer(stopped)

    loop = asyncio.get_running_loop()
    done_futures: list[asyncio.Future] = [loop.create_future(), loop.create_future()]
    for f in done_futures:
        f.set_result(None)

    # A single idle batch is enough to trigger the prune path.
    receiver = _FakeBatchReceiver([])

    mock_renewer = MagicMock()
    mock_renewer.__aenter__ = AsyncMock(return_value=mock_renewer)
    mock_renewer.__aexit__ = AsyncMock(return_value=False)
    mock_renewer.register = MagicMock()
    mock_renewer._futures = list(done_futures)

    factory = MagicMock(return_value=_AsyncCtx(receiver))
    stopped.is_set = lambda: len(receiver._batches) == 0  # type: ignore[method-assign]

    with patch(
        "wordlift_servicebus.consumers.AutoLockRenewer", return_value=mock_renewer
    ):
        await consumer.consume(
            factory,
            "queue=test",
            _TestMessage,
            AsyncMock(),
            10,
            _LOCK_RENEWAL_MAX_S,
        )

    assert mock_renewer._futures == [], "done futures must be pruned on idle poll"


@pytest.mark.asyncio
async def test_concurrent_consumer_prunes_tasks_and_futures_when_slots_full() -> None:
    """Both tasks and renewer futures must be pruned each time the slot-full branch fires.

    With max_concurrent=1 and one in-flight message, every loop iteration hits
    slots <= 0 until the task finishes.  Done futures pre-loaded on the renewer
    must be cleared on the first slot-full poll, not left to accumulate.
    """
    stopped = asyncio.Event()
    consumer = ConcurrentConsumer(stopped)

    loop = asyncio.get_running_loop()
    stale_future: asyncio.Future = loop.create_future()
    stale_future.set_result(None)

    # one real message then idle; max_concurrent=1 guarantees the slot-full branch fires
    receiver = _FakeBatchReceiver([[_payload(id="q1")], []])

    mock_renewer = MagicMock()
    mock_renewer.__aenter__ = AsyncMock(return_value=mock_renewer)
    mock_renewer.__aexit__ = AsyncMock(return_value=False)
    mock_renewer.register = MagicMock()
    mock_renewer._futures = [stale_future]

    factory = MagicMock(return_value=_AsyncCtx(receiver))
    stopped.is_set = lambda: len(receiver._batches) == 0  # type: ignore[method-assign]

    with patch(
        "wordlift_servicebus.consumers.AutoLockRenewer", return_value=mock_renewer
    ):
        await consumer.consume(
            factory,
            "queue=test",
            _TestMessage,
            AsyncMock(),
            1,  # max_concurrent=1 → slot fills immediately after first message
            _LOCK_RENEWAL_MAX_S,
            slot_poll_interval=0,  # asyncio.sleep(0) so the task gets CPU without delay
        )

    assert mock_renewer._futures == [], "done futures must be pruned on slot-full poll"
    assert receiver.complete_message.call_count == 1
    factory.assert_called_once()
