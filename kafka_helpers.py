"""Small Kafka helpers shared by anomaly_detector.py and storage_consumer.py."""

from confluent_kafka import TopicPartition


def next_offsets(messages):
    # Kafka's committed offset means "the next message I want", hence the + 1
    highest: dict = {}
    for msg in messages:
        key = (msg.topic(), msg.partition())
        highest[key] = max(highest.get(key, -1), msg.offset())
    return [TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in sorted(highest.items())]


def send_to_dead_letter(producer, topic, msg, error):
    """Park a message we can't process on a dead-letter topic instead of dropping it.

    The original bytes are kept unchanged, and headers record where it came from and why it
    failed, so it can be inspected (and replayed) later. The caller must flush the producer
    before committing offsets, otherwise a crash could lose the dead letter too.
    """
    producer.produce(
        topic,
        key=msg.key(),
        value=msg.value(),
        headers={
            "error": str(error)[:500],
            "source_topic": msg.topic(),
            "source_partition": str(msg.partition()),
            "source_offset": str(msg.offset()),
        },
    )


def consume_batch(consumer, max_messages, timeout):
    """Wait up to `timeout` seconds for the first message, then take whatever else is already
    waiting (up to `max_messages`) without waiting any longer.

    `consumer.consume(500, timeout=1.0)` on its own waits until it has 500 messages OR a full
    second has passed, so at ~1 trade/s every trade would sit for up to a second. This way a
    quiet stream adds no delay, and a busy one still gets big batches.
    """
    first = consumer.consume(num_messages=1, timeout=timeout)
    if not first:
        return []
    return first + consumer.consume(num_messages=max_messages - 1, timeout=0)
