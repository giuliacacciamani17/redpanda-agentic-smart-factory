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
    TELEMETRY_TOPIC,
)

# Configura il collegamento con Redpanda
def create_producer() -> Producer:
    configuration = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": f"machine-simulator-{MACHINE_ID}",
    }
    return Producer(configuration)

# Genera un evento
def create_telemetry_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": MACHINE_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(55.0, 75.0), 2),
        "vibration": round(random.uniform(1.0, 3.0), 2),
        "speed": random.randint(1200, 1500),
        "energy_consumption": round(random.uniform(80.0, 120.0), 2),
        "status": "RUNNING",
    }

# Gestisce il risultato della pubblicazione
def handle_delivery(error, message) -> None:
    if error is not None:
        print(f"Errore durante la pubblicazione: {error}")
        return

    print(
        "Evento pubblicato "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )

#Invia l'evento a Redpanda
def publish_event(producer: Producer, event: dict) -> None:
    producer.produce(
        topic=TELEMETRY_TOPIC,
        key=event["machine_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=handle_delivery,
    )
    producer.poll(0)

#Coordina il ciclo del servizio
def run_simulator() -> None:
    producer = create_producer()

    print(f"Simulatore avviato per {MACHINE_ID}")
    print(f"Broker: {KAFKA_BROKER}")
    print(f"Topic: {TELEMETRY_TOPIC}")

    try:
        while True:
            event = create_telemetry_event()
            publish_event(producer, event)

            print(json.dumps(event, indent=2))
            time.sleep(EVENT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Arresto del simulatore richiesto")
    finally:
        producer.flush()
        print("Simulatore arrestato correttamente")


if __name__ == "__main__":
    run_simulator()