import json
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer

from config import (
    CORRELATION_PREFIX,
    CORRELATION_START,
    EVENT_INTERVAL_SECONDS,
    KAFKA_BROKER,
    MACHINE_ID,
    RANDOM_SEED,
    TELEMETRY_TOPIC,
)


EVENT_PROFILES = (
    {
        "phase": "NORMAL",
        "temperature": 60.0,
        "vibration": 1.5,
        "speed": 1400,
        "energy_consumption": 95.0,
    },
    {
        "phase": "MONITORING",
        "temperature": 80.0,
        "vibration": 5.5,
        "speed": 1420,
        "energy_consumption": 112.0,
    },
    {
        "phase": "DEGRADING",
        "temperature": 94.0,
        "vibration": 6.9,
        "speed": 1450,
        "energy_consumption": 128.0,
    },
    {
        "phase": "SEVERE",
        "temperature": 100.0,
        "vibration": 9.0,
        "speed": 1470,
        "energy_consumption": 140.0,
    },
    {
        "phase": "CRITICAL",
        "temperature": 105.0,
        "vibration": 10.0,
        "speed": 1500,
        "energy_consumption": 150.0,
    },
)

running = True


def create_producer() -> Producer:
    configuration = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": f"machine-simulator-{MACHINE_ID}",
    }

    return Producer(configuration)


def generate_measurements(
    profile: dict[str, Any],
    random_generator: random.Random,
) -> dict[str, Any]:
    return {
        "temperature": round(
            profile["temperature"]
            + random_generator.uniform(-0.4, 0.4),
            2,
        ),
        "vibration": round(
            profile["vibration"]
            + random_generator.uniform(-0.08, 0.08),
            2,
        ),
        "speed": profile["speed"]
        + random_generator.randint(-10, 10),
        "energy_consumption": round(
            profile["energy_consumption"]
            + random_generator.uniform(-1.0, 1.0),
            2,
        ),
        "phase": profile["phase"],
    }


def create_correlation_id(
    sequence_number: int,
) -> str:
    correlation_number = (
        CORRELATION_START
        + sequence_number
        - 1
    )

    return (
        f"{CORRELATION_PREFIX}-"
        f"{correlation_number}"
    )


def create_telemetry_event(
    measurements: dict[str, Any],
    sequence_number: int,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "correlation_id": create_correlation_id(
            sequence_number
        ),
        "machine_id": MACHINE_ID,
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "sequence_number": sequence_number,
        **measurements,
        "status": "RUNNING",
        "simulation_mode": "CONTROLLED_RANDOM",
    }


def handle_delivery(
    error,
    message,
) -> None:
    if error is not None:
        print(
            f"Errore durante la pubblicazione: {error}",
            flush=True,
        )
        return

    print(
        "Evento pubblicato "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}",
        flush=True,
    )


def publish_event(
    producer: Producer,
    event: dict[str, Any],
) -> None:
    producer.produce(
        topic=TELEMETRY_TOPIC,
        key=event["machine_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=handle_delivery,
    )

    producer.poll(0)


def handle_shutdown(
    signum,
    frame,
) -> None:
    del signum, frame

    global running
    running = False

    print(
        "Arresto del simulatore richiesto",
        flush=True,
    )


def run_simulator() -> None:
    producer = create_producer()
    random_generator = random.Random(
        RANDOM_SEED
    )

    print(
        f"Simulatore avviato per {MACHINE_ID}",
        flush=True,
    )
    print(
        f"Broker: {KAFKA_BROKER}",
        flush=True,
    )
    print(
        f"Topic: {TELEMETRY_TOPIC}",
        flush=True,
    )
    print(
        f"Eventi pianificati: {len(EVENT_PROFILES)}",
        flush=True,
    )
    print(
        f"Seed pseudocasuale: {RANDOM_SEED}",
        flush=True,
    )

    try:
        for sequence_number, profile in enumerate(
            EVENT_PROFILES,
            start=1,
        ):
            if not running:
                break

            measurements = generate_measurements(
                profile=profile,
                random_generator=random_generator,
            )

            event = create_telemetry_event(
                measurements=measurements,
                sequence_number=sequence_number,
            )

            publish_event(
                producer=producer,
                event=event,
            )

            producer.flush(10)

            print(
                f"Evento={sequence_number} "
                f"correlation_id={event['correlation_id']} "
                f"fase={event['phase']} "
                f"temperatura={event['temperature']} "
                f"vibrazione={event['vibration']}",
                flush=True,
            )

            if sequence_number < len(EVENT_PROFILES):
                time.sleep(EVENT_INTERVAL_SECONDS)

    finally:
        producer.flush(10)

        print(
            "Simulazione completata: "
            "5 eventi pubblicati",
            flush=True,
        )


def main() -> None:
    signal.signal(
        signal.SIGINT,
        handle_shutdown,
    )
    signal.signal(
        signal.SIGTERM,
        handle_shutdown,
    )

    run_simulator()


if __name__ == "__main__":
    main()