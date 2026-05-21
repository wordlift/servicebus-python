"""Tests for wordlift_servicebus.handlers — handle_one behaviour."""

from __future__ import annotations

import json
from typing import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from azure.servicebus.exceptions import MessageLockLostError

from wordlift_servicebus import handle_one

from .helpers import (
    _LOCK_RENEWAL_MAX_S,
    _FakeReceiver,
    _TestMessage,
    _payload,
)


async def _run(
    receiver: _FakeReceiver,
    handler: AsyncMock,
    *,
    renewer: MagicMock | None = None,
    on_retry: Callable[[Exception], Awaitable[bool]] | None = None,
    source: str = "queue=test",
) -> None:
    if renewer is None:
        renewer = MagicMock()
    async for raw in receiver:
        await handle_one(
            receiver,
            renewer,
            raw,
            _TestMessage,
            handler,
            source,
            _LOCK_RENEWAL_MAX_S,
            on_retry=on_retry,
        )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_completes_message() -> None:
    handler = AsyncMock()
    receiver = _FakeReceiver([_payload()])

    await _run(receiver, handler)

    handler.assert_awaited_once()
    receiver.complete_message.assert_awaited_once()
    receiver.dead_letter_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_processes_multiple_messages_in_order() -> None:
    processed: list[str] = []

    async def handler(msg: _TestMessage) -> None:
        processed.append(msg.id)

    receiver = _FakeReceiver([_payload(id="r1"), _payload(id="r2")])
    await _run(receiver, AsyncMock(side_effect=handler))

    assert processed == ["r1", "r2"]
    assert receiver.complete_message.call_count == 2


@pytest.mark.asyncio
async def test_registers_lock_renewer_per_message() -> None:
    receiver = _FakeReceiver([_payload(), _payload(id="r2")])
    renewer = MagicMock()

    await _run(receiver, AsyncMock(), renewer=renewer)

    assert renewer.register.call_count == 2


@pytest.mark.asyncio
async def test_lock_lost_on_complete_does_not_raise() -> None:
    receiver = _FakeReceiver([_payload()], complete_side_effect=MessageLockLostError())

    await _run(receiver, AsyncMock())


# ---------------------------------------------------------------------------
# on_retry callback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_retry_returning_true_completes_message() -> None:
    exc = RuntimeError("transient")
    handler = AsyncMock(side_effect=exc)
    receiver = _FakeReceiver([_payload()])
    received: list[Exception] = []

    async def _on_retry(e: Exception) -> bool:
        received.append(e)
        return True

    await _run(receiver, handler, on_retry=_on_retry)

    assert received == [exc]
    receiver.complete_message.assert_awaited_once()
    receiver.dead_letter_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_retry_returning_false_dead_letters_message() -> None:
    handler = AsyncMock(side_effect=RuntimeError("permanent"))
    receiver = _FakeReceiver([_payload()])

    async def _on_retry(exc: Exception) -> bool:
        return False

    await _run(receiver, handler, on_retry=_on_retry)

    receiver.dead_letter_message.assert_awaited_once()
    receiver.complete_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_retry_true_lock_lost_does_not_raise() -> None:
    handler = AsyncMock(side_effect=RuntimeError("transient"))
    receiver = _FakeReceiver([_payload()], complete_side_effect=MessageLockLostError())

    async def _on_retry(exc: Exception) -> bool:
        return True

    await _run(receiver, handler, on_retry=_on_retry)


# ---------------------------------------------------------------------------
# Dead-lettering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handler_exception_dead_letters_message() -> None:
    handler = AsyncMock(side_effect=RuntimeError("unexpected"))
    receiver = _FakeReceiver([_payload()])

    await _run(receiver, handler)

    receiver.dead_letter_message.assert_awaited_once()
    receiver.complete_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_parse_error_dead_letters_message() -> None:
    handler = AsyncMock()
    receiver = _FakeReceiver([json.dumps({"_class": "UnknownType", "_version": 1})])

    await _run(receiver, handler)

    receiver.dead_letter_message.assert_awaited_once()
    handler.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_json_dead_letters_message() -> None:
    handler = AsyncMock()
    receiver = _FakeReceiver(["not-valid-json{{{"])

    await _run(receiver, handler)

    receiver.dead_letter_message.assert_awaited_once()
    handler.assert_not_awaited()


@pytest.mark.asyncio
async def test_lock_lost_on_dead_letter_does_not_raise() -> None:
    handler = AsyncMock(side_effect=RuntimeError("unexpected"))
    receiver = _FakeReceiver(
        [_payload()], dead_letter_side_effect=MessageLockLostError()
    )

    await _run(receiver, handler)


@pytest.mark.asyncio
async def test_dead_letter_does_not_block_subsequent_messages() -> None:
    processed: list[str] = []

    async def handler(msg: _TestMessage) -> None:
        if msg.id == "fail":
            raise RuntimeError("boom")
        processed.append(msg.id)

    receiver = _FakeReceiver([_payload(id="fail"), _payload(id="ok")])
    await _run(receiver, AsyncMock(side_effect=handler))

    assert processed == ["ok"]
    assert receiver.dead_letter_message.call_count == 1
    assert receiver.complete_message.call_count == 1
