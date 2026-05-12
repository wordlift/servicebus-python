from __future__ import annotations

from typing import Any, Protocol, Self


class ParseableMessage(Protocol):
    @classmethod
    def from_payload(cls, data: dict[str, Any]) -> Self: ...


class SerializableMessage(Protocol):
    def model_dump_json(self, *, by_alias: bool = False) -> str: ...


class BusMessage(ParseableMessage, SerializableMessage, Protocol): ...
