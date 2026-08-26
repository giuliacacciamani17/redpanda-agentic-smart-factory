import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import (
    EVENT_INTERVAL_SECONDS,
    KAFKA_BROKER,
    MACHINE_ID,
    SIMULATION_MODE,
    TELEMETRY_TOPIC,
)


NORMAL_EVENTS = 6
DEGRADING_EVENTS = 12
CRITICAL_EVENTS = 4
RECOVERY_EVENTS = 10

CYCLE_LENGTH = (
    NORMAL_EVENTS
    + DEGRADING_EVENTS
    + CRITICAL_EVENTS
    + RECOVERY_EVENTS
)


def create_producer() -> Producer:
    configuration = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": f"machine-simulator-{MACHINE_ID}",
    }

    return Producer(configuration)


def create_normal_measurements() -> dict:
    return {
        "temperature": round(random.uniform(57.0, 63.0), 2),
        "vibration": round(random.uniform(1.0, 1.8), 2),
        "speed": random.randint(1350, 1450),
        "energy_consumption": round(
            random.uniform(85.0, 105.0),
            2,
        ),
        "phase": "NORMAL",
    }


def create_degrading_measurements(step: int) -> dict:
    return {
        "temperature": round(64.0 + step * 2.0, 2),
        "vibration": round(1.8 + step * 0.42, 2),
        "speed": random.randint(1350, 1450),
        "energy_consumption": round(105.0 + step * 2.0, 2),
        "phase": "DEGRADING",
    }


def create_critical_measurements() -> dict:
    return {
        "temperature": round(random.uniform(93.0, 97.0), 2),
        "vibration": round(random.uniform(7.2, 8.0), 2),
        "speed": random.randint(1400, 1500),
        "energy_consumption": round(
            random.uniform(130.0, 145.0),
            2,
        ),
        "phase": "CRITICAL",
    }


def create_recovery_measurements(step: int) -> dict:
    progress = step / RECOVERY_EVENTS

    return {
        "temperature": round(94.0 - progress * 34.0, 2),
        "vibration": round(7.4 - progress * 5.9, 2),
        "speed": round(900 + progress * 450),
        "energy_consumption": round(
            130.0 - progress * 35.0,
            2,
        ),
        "phase": "RECOVERY",
    }


def create_scenario_measurements(
    event_number: int,
) -> dict:
    cycle_position = (event_number - 1) % CYCLE_LENGTH

    if cycle_position < NORMAL_EVENTS:
        return create_normal_measurements()

    cycle_position -= NORMAL_EVENTS

    if cycle_position < DEGRADING_EVENTS:
        return create_degrading_measurements(
            cycle_position + 1
        )

    cycle_position -= DEGRADING_EVENTS

    if cycle_position < CRITICAL_EVENTS:
        return create_critical_measurements()

    cycle_position -= CRITICAL_EVENTS

    return create_recovery_measurements(
        cycle_position + 1
    )


def create_measurements(event_number: int) -> dict:
    if SIMULATION_MODE == "SCENARIO":
        return create_scenario_measurements(event_number)

    return create_normal_measurements()


def create_telemetry_event(
    event_number: int,
) -> dict:
    measurements = create_measurements(event_number)

    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": MACHINE_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **measurements,
        "status": "RUNNING",
        "simulation_mode": SIMULATION_MODE,
    }


def handle_delivery(error, message) -> None:
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
    event: dict,
) -> None:
    producer.produce(
        topic=TELEMETRY_TOPIC,
        key=event["machine_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=handle_delivery,
    )

    producer.poll(0)


def run_simulator() -> None:
    producer = create_producer()
    event_number = 0

    print(f"Simulatore avviato per {MACHINE_ID}", flush=True)
    print(f"Broker: {KAFKA_BROKER}", flush=True)
    print(f"Topic: {TELEMETRY_TOPIC}", flush=True)
    print(f"Modalità: {SIMULATION_MODE}", flush=True)

    try:
        while True:
            event_number += 1
            event = create_telemetry_event(event_number)

            publish_event(producer, event)

            print(
                f"Fase={event['phase']} "
                f"temperatura={event['temperature']} "
                f"vibrazione={event['vibration']}",
                flush=True,
            )

            time.sleep(EVENT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print(
            "Arresto del simulatore richiesto",
            flush=True,
        )

    finally:
        producer.flush()

        print(
            "Simulatore arrestato correttamente",
            flush=True,
        )


if __name__ == "__main__":
    run_simulator()