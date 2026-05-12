from __future__ import annotations

import json
import logging
from typing import Awaitable, Callable, Type, TypeVar

from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import AutoLockRenewer, ServiceBusReceiver
from azure.servicebus.exceptions import MessageLockLostError

from .protocols import ParseableMessage

logger = logging.getLogger(__name__)

M = TypeVar("M", bound=ParseableMessage)


async def handle_one(
    receiver: ServiceBusReceiver,
    renewer: AutoLockRenewer,
    raw: ServiceBusReceivedMessage,
    message_type: Type[M],
    handler: Callable[[M], Awaitable[None]],
    source: str,
    max_lock_renewal_duration: float,
    *,
    on_retry: Callable[[Exception], Awaitable[bool]] | None = None,
) -> None:
    """Process a single raw SB message: parse, dispatch, ack/nack/dead-letter."""
    renewer.register(receiver, raw, max_lock_renewal_duration=max_lock_renewal_duration)
    _class = "<missing>"
    try:
        payload = json.loads(str(raw))
        _class = payload.get("_class", "<missing>")
        logger.debug("Received _class=%s from %s", _class, source)
        msg = message_type.from_payload(payload)
        await handler(msg)
        try:
            await receiver.complete_message(raw)
            logger.debug("Completed _class=%s from %s", _class, source)
        except MessageLockLostError:
            logger.warning(
                "Lock lost before complete _class=%s from %s — message will be redelivered",
                _class,
                source,
            )
    except Exception as exc:
        if on_retry is not None and await on_retry(exc):
            try:
                await receiver.complete_message(raw)
            except MessageLockLostError:
                logger.warning(
                    "Lock lost before complete after requeue _class=%s from %s",
                    _class,
                    source,
                )
        else:
            logger.exception("Dead-lettering _class=%s from %s", _class, source)
            try:
                await receiver.dead_letter_message(raw)
            except MessageLockLostError:
                logger.warning(
                    "Lock lost before dead-letter _class=%s from %s — message will be redelivered",
                    _class,
                    source,
                )
