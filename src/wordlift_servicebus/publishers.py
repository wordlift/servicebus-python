from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

from azure.servicebus import ServiceBusMessage, ServiceBusMessageBatch
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.exceptions import MessageSizeExceededError

from .protocols import SerializableMessage

logger = logging.getLogger(__name__)

# Since azure-servicebus 7.14.3 (Azure/azure-sdk-for-python#42598), size_in_bytes
# leaves out the envelope properties copied from a batch's first message: 56 bytes
# by default, up to ~1.6 KB with 128-character non-ASCII IDs. Batches are capped this
# far below the link limit. Reported upstream at
# https://github.com/Azure/azure-sdk-for-python/issues/49865; this can be removed
# once a fixed SDK version is the minimum we support.
_BATCH_SIZE_HEADROOM_BYTES = 2 * 1024


async def publish(
    client: ServiceBusClient, queue_name: str, message: SerializableMessage
) -> None:
    async with client.get_queue_sender(queue_name) as sender:
        await sender.send_messages(
            ServiceBusMessage(message.model_dump_json(by_alias=True))
        )
        logger.debug("Published %s to queue=%s", type(message).__name__, queue_name)


def _try_add(batch: ServiceBusMessageBatch, body: str) -> bool:
    try:
        batch.add_message(ServiceBusMessage(body))
    except MessageSizeExceededError:
        return False
    return True


async def publish_batch(
    client: ServiceBusClient,
    queue_name: str,
    messages: Sequence[SerializableMessage],
) -> None:
    """Send messages in order, in batches 2 KiB under the link limit.

    A message that doesn't fit an empty capped batch is sent on its own.
    """
    async with client.get_queue_sender(queue_name) as sender:
        # A default batch is sized to the link maximum.
        link_max = (await sender.create_message_batch()).max_size_in_bytes
        capped_size = link_max - _BATCH_SIZE_HEADROOM_BYTES

        batch = await sender.create_message_batch(max_size_in_bytes=capped_size)
        for msg in messages:
            body = msg.model_dump_json(by_alias=True)
            if _try_add(batch, body):
                continue
            if len(batch) > 0:
                await sender.send_messages(batch)
                logger.debug(
                    "Sent full batch size=%d to queue=%s", len(batch), queue_name
                )
                batch = await sender.create_message_batch(max_size_in_bytes=capped_size)
                if _try_add(batch, body):
                    continue
            # Too big for an empty capped batch. Adding it to an uncapped batch raises
            # MessageSizeExceededError locally if it exceeds the link limit.
            (await sender.create_message_batch()).add_message(ServiceBusMessage(body))
            await sender.send_messages(ServiceBusMessage(body))
            logger.debug("Sent oversized message alone to queue=%s", queue_name)
        if len(batch) > 0:
            await sender.send_messages(batch)
            logger.debug("Sent batch size=%d to queue=%s", len(batch), queue_name)


async def publish_topic(
    client: ServiceBusClient,
    topic_name: str,
    message: SerializableMessage,
) -> None:
    async with client.get_topic_sender(topic_name) as sender:
        await sender.send_messages(
            ServiceBusMessage(message.model_dump_json(by_alias=True))
        )
        logger.debug("Published %s to topic=%s", type(message).__name__, topic_name)


async def publish_scheduled(
    client: ServiceBusClient,
    queue_name: str,
    message: SerializableMessage,
    delay_seconds: float,
) -> None:
    scheduled_time = datetime.now(tz=timezone.utc) + timedelta(seconds=delay_seconds)
    async with client.get_queue_sender(queue_name) as sender:
        sb_msg = ServiceBusMessage(
            message.model_dump_json(by_alias=True),
            scheduled_enqueue_time_utc=scheduled_time,
        )
        await sender.send_messages(sb_msg)
        logger.debug(
            "Scheduled %s to queue=%s delay=%.1fs",
            type(message).__name__,
            queue_name,
            delay_seconds,
        )
