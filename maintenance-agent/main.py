import json
import signal
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)

from config import (
    AGENT_ID,
    COMMANDS_TOPIC,
    CONSUMER_GROUP,
    DECISIONS_TOPIC,
    KAFKA_BROKER,
    STATE_WINDOW_SIZE,
    TELEMETRY_TOPIC,
)
from policy import (
    MONITOR,
    NO_ACTION,
    explain_action,
    select_action,
)
from risk_engine import calculate_risk
from state import MachineState


machine_states: dict[str, MachineState] = {}
running = True


def create_consumer() -> Consumer:
    configuration = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    return Consumer(configuration)


def create_producer() -> Producer:
    configuration = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": AGENT_ID,
    }

    return Producer(configuration)


def deserialize_telemetry(
    message_value: bytes,
) -> dict[str, Any]:
    telemetry = json.loads(
        message_value.decode("utf-8")
    )

    required_fields = {
        "event_id",
        "correlation_id",
        "machine_id",
        "timestamp",
        "temperature",
        "vibration",
    }

    missing_fields = required_fields - telemetry.keys()

    if missing_fields:
        missing_names = ", ".join(
            sorted(missing_fields)
        )

        raise ValueError(
            f"Campi mancanti: {missing_names}"
        )

    return telemetry


def get_machine_state(
    machine_id: str,
) -> MachineState:
    if machine_id not in machine_states:
        machine_states[machine_id] = MachineState(
            machine_id=machine_id,
            window_size=STATE_WINDOW_SIZE,
        )

    return machine_states[machine_id]


def create_decision_event(
    telemetry: dict[str, Any],
    state: MachineState,
    risk_score: float,
    action: str,
) -> dict[str, Any]:
    return {
        "decision_id": str(uuid4()),
        "agent_id": AGENT_ID,
        "source_event_id": telemetry["event_id"],
        "machine_id": telemetry["machine_id"],
        "correlation_id": telemetry["correlation_id"],
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "risk_score": risk_score,
        "average_temperature": round(
            state.average_temperature(),
            2,
        ),
        "average_vibration": round(
            state.average_vibration(),
            2,
        ),
        "temperature_is_rising": (
            state.temperature_is_rising()
        ),
        "vibration_is_rising": (
            state.vibration_is_rising()
        ),
        "previous_action": state.last_action,
        "selected_action": action,
        "reason": explain_action(
            action,
            risk_score,
        ),
    }


def create_command_event(
    decision: dict[str, Any],
) -> dict[str, Any]:
    return {
        "command_id": str(uuid4()),
        "decision_id": decision["decision_id"],
        "correlation_id": decision["correlation_id"],
        "agent_id": decision["agent_id"],
        "machine_id": decision["machine_id"],
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "action": decision["selected_action"],
        "risk_score": decision["risk_score"],
        "reason": decision["reason"],
    }


def delivery_report(
    error,
    message,
) -> None:
    if error is not None:
        print(
            f"Errore di pubblicazione: {error}",
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
    topic: str,
    machine_id: str,
    event: dict[str, Any],
) -> None:
    producer.produce(
        topic=topic,
        key=machine_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )

    producer.poll(0)


def should_publish_command(
    action: str,
    previous_action: str,
) -> bool:
    requires_intervention = action not in {
        NO_ACTION,
        MONITOR,
    }

    action_has_changed = action != previous_action

    return (
        requires_intervention
        and action_has_changed
    )


def process_telemetry(
    telemetry: dict[str, Any],
    producer: Producer,
) -> None:
    machine_id = telemetry["machine_id"]
    state = get_machine_state(machine_id)

    state.update(telemetry)

    risk_score = calculate_risk(state)
    action = select_action(risk_score)

    decision = create_decision_event(
        telemetry=telemetry,
        state=state,
        risk_score=risk_score,
        action=action,
    )

    publish_event(
        producer=producer,
        topic=DECISIONS_TOPIC,
        machine_id=machine_id,
        event=decision,
    )

    if should_publish_command(
        action=action,
        previous_action=state.last_action,
    ):
        command = create_command_event(decision)

        publish_event(
            producer=producer,
            topic=COMMANDS_TOPIC,
            machine_id=machine_id,
            event=command,
        )

    print(
        f"Macchina={machine_id} "
        f"rischio={risk_score:.2f} "
        f"azione_precedente={state.last_action} "
        f"azione_selezionata={action}",
        flush=True,
    )

    state.last_action = action


def handle_shutdown(
    signum,
    frame,
) -> None:
    del signum, frame

    global running
    running = False

    print(
        "Arresto del Maintenance Agent richiesto",
        flush=True,
    )


def run_agent() -> None:
    consumer = create_consumer()
    producer = create_producer()

    consumer.subscribe([TELEMETRY_TOPIC])

    print(
        f"Maintenance Agent avviato: {AGENT_ID}",
        flush=True,
    )
    print(
        f"Broker: {KAFKA_BROKER}",
        flush=True,
    )
    print(
        f"Topic osservato: {TELEMETRY_TOPIC}",
        flush=True,
    )

    try:
        while running:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                if (
                    message.error().code()
                    == KafkaError._PARTITION_EOF
                ):
                    continue

                raise KafkaException(
                    message.error()
                )

            try:
                telemetry = deserialize_telemetry(
                    message.value()
                )

                process_telemetry(
                    telemetry=telemetry,
                    producer=producer,
                )

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            except (
                json.JSONDecodeError,
                KeyError,
                TypeError,
                ValueError,
            ) as error:
                print(
                    f"Evento non valido: {error}",
                    flush=True,
                )

    finally:
        producer.flush(10)
        consumer.close()

        print(
            "Maintenance Agent arrestato correttamente",
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

    run_agent()


if __name__ == "__main__":
    main()