from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from typing import Any, Awaitable, Callable, Type, TypeVar

from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import AutoLockRenewer, ServiceBusReceiver

from .handlers import handle_one
from .protocols import ParseableMessage


_CONCURRENT_RECEIVER_MAX_WAIT_S = 5.0
# Pause when all slots are occupied to avoid rapid-fire receive_messages calls to Azure SB.
_CONCURRENT_SLOT_POLL_INTERVAL = 0.5

logger = logging.getLogger(__name__)

M = TypeVar("M", bound=ParseableMessage)


class MessageConsumer(ABC):
    def __init__(self, stopped: asyncio.Event) -> None:
        self._stopped = stopped

    @abstractmethod
    async def consume(self, *args: Any, **kwargs: Any) -> None: ...


class SequentialConsumer(MessageConsumer):
    async def consume(
        self,
        receiver_factory: Callable[[], AbstractAsyncContextManager[ServiceBusReceiver]],
        source: str,
        message_type: Type[M],
        handler: Callable[[M], Awaitable[None]],
        max_lock_renewal_duration: float,
        *,
        on_retry: Callable[[Exception], Awaitable[bool]] | None = None,
    ) -> None:
        async with AutoLockRenewer() as renewer:
            async with receiver_factory() as receiver:
                logger.debug("Waiting for messages on %s", source)
                async for raw in receiver:
                    await handle_one(
                        receiver,
                        renewer,
                        raw,
                        message_type,
                        handler,
                        source,
                        max_lock_renewal_duration,
                        on_retry=on_retry,
                    )


class BatchConsumer(MessageConsumer):
    async def consume(
        self,
        receiver_factory: Callable[[], AbstractAsyncContextManager[ServiceBusReceiver]],
        source: str,
        message_type: Type[M],
        handler: Callable[[list[M]], Awaitable[None]],
        batch_size: int,
        batch_flush_timeout: float,
        max_lock_renewal_duration: float,
    ) -> None:
        async with AutoLockRenewer() as renewer:
            async with receiver_factory() as receiver:
                logger.debug("Waiting for batch messages on %s", source)
                await self._run_loop(
                    receiver,
                    renewer,
                    message_type,
                    handler,
                    source,
                    batch_size,
                    batch_flush_timeout,
                    max_lock_renewal_duration,
                )

    async def _run_loop(
        self,
        receiver: ServiceBusReceiver,
        renewer: AutoLockRenewer,
        message_type: Type[M],
        handler: Callable[[list[M]], Awaitable[None]],
        source: str,
        batch_size: int,
        batch_flush_timeout: float,
        max_lock_renewal_duration: float,
    ) -> None:
        last_idle_log = time.monotonic()
        last_activity = last_idle_log
        while not self._stopped.is_set():
            raw_batch = await receiver.receive_messages(
                max_message_count=batch_size, max_wait_time=batch_flush_timeout
            )
            if not raw_batch:
                now = time.monotonic()
                idle_for = now - last_activity
                if idle_for >= batch_flush_timeout and now - last_idle_log >= 30.0:
                    logger.debug(
                        "No messages received from %s in the last %.1fs",
                        source,
                        idle_for,
                    )
                    last_idle_log = now
                continue

            for raw in raw_batch:
                renewer.register(
                    receiver, raw, max_lock_renewal_duration=max_lock_renewal_duration
                )

            parsed_batch: list[M] = []
            failed: list[ServiceBusReceivedMessage] = []
            for raw in raw_batch:
                _class = "<missing>"
                try:
                    payload = json.loads(str(raw))
                    _class = payload.get("_class", "<missing>")
                    parsed_batch.append(message_type.from_payload(payload))
                except Exception:
                    logger.exception(
                        "Dead-lettering _class=%s from %s (parse error)", _class, source
                    )
                    failed.append(raw)

            for raw in failed:
                await receiver.dead_letter_message(raw)

            if not parsed_batch:
                continue

            try:
                await handler(parsed_batch)
                for raw in raw_batch:
                    if raw not in failed:
                        await receiver.complete_message(raw)
                last_activity = time.monotonic()
                logger.info("Completed batch of %d from %s", len(parsed_batch), source)
            except Exception:
                logger.exception(
                    "Batch handler failed, dead-lettering %d messages from %s",
                    len(parsed_batch),
                    source,
                )
                for raw in raw_batch:
                    if raw not in failed:
                        await receiver.dead_letter_message(raw)


class ConcurrentConsumer(MessageConsumer):
    async def consume(
        self,
        receiver_factory: Callable[[], AbstractAsyncContextManager[ServiceBusReceiver]],
        source: str,
        message_type: Type[M],
        handler: Callable[[M], Awaitable[None]],
        max_concurrent: int,
        max_lock_renewal_duration: float,
        receiver_max_wait_time: float = _CONCURRENT_RECEIVER_MAX_WAIT_S,
        slot_poll_interval: float = _CONCURRENT_SLOT_POLL_INTERVAL,
        *,
        on_retry: Callable[[Exception], Awaitable[bool]] | None = None,
    ) -> None:
        while not self._stopped.is_set():
            async with AutoLockRenewer() as renewer:
                async with receiver_factory() as receiver:
                    logger.debug(
                        "Listening for messages on %s max_concurrent=%d",
                        source,
                        max_concurrent,
                    )
                    in_flight = 0
                    tasks: list[asyncio.Task[None]] = []
                    while not self._stopped.is_set():
                        slots = max_concurrent - in_flight
                        if slots <= 0:
                            await asyncio.sleep(slot_poll_interval)
                            tasks = [t for t in tasks if not t.done()]
                            in_flight = len(tasks)
                            continue

                        batch = await receiver.receive_messages(
                            max_message_count=slots,
                            max_wait_time=receiver_max_wait_time,
                        )
                        if not batch:
                            tasks = [t for t in tasks if not t.done()]
                            continue

                        for r in batch:
                            in_flight += 1

                            async def _task(
                                raw: ServiceBusReceivedMessage = r,
                            ) -> None:
                                nonlocal in_flight
                                try:
                                    await handle_one(
                                        receiver,
                                        renewer,
                                        raw,
                                        message_type,
                                        handler,
                                        source,
                                        max_lock_renewal_duration,
                                        on_retry=on_retry,
                                    )
                                finally:
                                    in_flight -= 1

                            tasks.append(asyncio.create_task(_task()))

                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
