from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.exceptions import MessageSizeExceededError

from .protocols import SerializableMessage

logger = logging.getLogger(__name__)


async def publish(
    client: ServiceBusClient, queue_name: str, message: SerializableMessage
) -> None:
    async with client.get_queue_sender(queue_name) as sender:
        await sender.send_messages(
            ServiceBusMessage(message.model_dump_json(by_alias=True))
        )
        logger.debug("Published %s to queue=%s", type(message).__name__, queue_name)


async def publish_batch(
    client: ServiceBusClient,
    queue_name: str,
    messages: list[SerializableMessage],
) -> None:
    """Send messages using a Service Bus batch, flushing when the 256 KB limit is hit."""
    async with client.get_queue_sender(queue_name) as sender:
        batch = await sender.create_message_batch()
        for msg in messages:
            sb_msg = ServiceBusMessage(msg.model_dump_json(by_alias=True))
            try:
                batch.add_message(sb_msg)
            except MessageSizeExceededError:
                await sender.send_messages(batch)
                logger.debug("Sent full batch to queue=%s", queue_name)
                batch = await sender.create_message_batch()
                batch.add_message(sb_msg)
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
