"""Shared test infrastructure — stubs, fakes, and helpers."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

_LOCK_RENEWAL_MAX_S = 300.0


class _TestMessage:
    def __init__(self, id: str = "msg-1") -> None:
        self.id = id

    @classmethod
    def from_payload(cls, data: dict[str, Any]) -> "_TestMessage":
        if data.get("_class") != "TestMessage":
            raise ValueError(f"Unexpected _class={data.get('_class')!r}")
        return cls(id=str(data.get("id", "msg-1")))

    def model_dump_json(self, *, by_alias: bool = False) -> str:
        return json.dumps({"_class": "TestMessage", "_version": 1, "id": self.id})


def _payload(**overrides: object) -> str:
    base: dict = {"_version": 1, "_class": "TestMessage", "id": "msg-1"}
    base.update(overrides)
    return json.dumps(base)


class _FakeReceiver:
    """Async-iterable fake that yields raw string payloads."""

    def __init__(
        self,
        payloads: list[str],
        complete_side_effect: BaseException | None = None,
        dead_letter_side_effect: BaseException | None = None,
    ) -> None:
        self._payloads = payloads
        self.complete_message = AsyncMock(side_effect=complete_side_effect)
        self.dead_letter_message = AsyncMock(side_effect=dead_letter_side_effect)

    def __aiter__(self):  # type: ignore[no-untyped-def]
        return self._gen()

    async def _gen(self):  # type: ignore[no-untyped-def]
        for p in self._payloads:
            yield p


class _FakeBatchReceiver:
    """Fake receiver that returns batches via receive_messages."""

    def __init__(self, batches: list[list[str]]) -> None:
        self._batches = list(batches) + [[]]  # trailing empty batch triggers stop
        self.complete_message = AsyncMock()
        self.dead_letter_message = AsyncMock()
        self._receive_calls: list[dict] = []

    async def receive_messages(
        self, max_message_count: int, max_wait_time: float
    ) -> list[str]:
        self._receive_calls.append(
            {"max_message_count": max_message_count, "max_wait_time": max_wait_time}
        )
        if not self._batches:
            return []
        result, remaining = (
            self._batches[0][:max_message_count],
            self._batches[0][max_message_count:],
        )
        if remaining:
            self._batches[0] = remaining
        else:
            self._batches.pop(0)
        return result


class _AsyncCtx:
    """Async context manager that returns a given value on __aenter__."""

    def __init__(self, value: object) -> None:
        self._value = value

    async def __aenter__(self) -> object:
        return self._value

    async def __aexit__(self, *exc: object) -> None:
        pass


def _fake_sender_ctx(sender: AsyncMock) -> AsyncMock:
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=sender)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


def _fake_client(sender: AsyncMock) -> MagicMock:
    client = MagicMock()
    client.get_queue_sender.return_value = _fake_sender_ctx(sender)
    return client


def _serializable(json_str: str) -> MagicMock:
    msg = MagicMock()
    msg.model_dump_json.return_value = json_str
    return msg
