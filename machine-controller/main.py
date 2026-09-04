import json
import signal
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)

from config import (
    COMMAND_RESULTS_TOPIC,
    COMMANDS_TOPIC,
    CONSUMER_GROUP,
    CONTROLLER_ID,
    CONTROLLER_MODE,
    KAFKA_BROKER,
)
from controller import MachineController


REQUIRED_COMMAND_FIELDS = {
    "command_id",
    "decision_id",
    "correlation_id",
    "machine_id",
    "action",
    "risk_score",
}

running = True


def create_consumer() -> Consumer:
    configuration = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }

    return Consumer(configuration)


def create_producer() -> Producer:
    configuration = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": CONTROLLER_ID,
    }

    return Producer(configuration)


def deserialize_command(
    message_value: bytes,
) -> dict[str, Any]:
    command = json.loads(
        message_value.decode("utf-8")
    )

    if not isinstance(command, dict):
        raise TypeError(
            "Il comando deve essere un oggetto JSON"
        )

    missing_fields = (
        REQUIRED_COMMAND_FIELDS
        - command.keys()
    )

    if missing_fields:
        missing_names = ", ".join(
            sorted(missing_fields)
        )

        raise ValueError(
            f"Campi mancanti: {missing_names}"
        )

    return command


def create_command_result(
    command: dict[str, Any],
    execution_result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "result_id": str(uuid.uuid4()),
        "command_id": command["command_id"],
        "decision_id": command["decision_id"],
        "correlation_id": command[
            "correlation_id"
        ],
        "controller_id": CONTROLLER_ID,
        "agent_id": command.get("agent_id"),
        "machine_id": command["machine_id"],
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "action": command["action"],
        "risk_score": command["risk_score"],
        **execution_result,
    }


def delivery_report(
    error,
    message,
) -> None:
    if error is not None:
        print(
            "Errore durante la pubblicazione: "
            f"{error}",
            flush=True,
        )
        return

    print(
        "Risultato pubblicato "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}",
        flush=True,
    )


def publish_result(
    producer: Producer,
    command_result: dict[str, Any],
) -> None:
    producer.produce(
        topic=COMMAND_RESULTS_TOPIC,
        key=command_result[
            "machine_id"
        ].encode("utf-8"),
        value=json.dumps(
            command_result
        ).encode("utf-8"),
        callback=delivery_report,
    )

    producer.poll(0)


def process_command(
    message_value: bytes,
    producer: Producer,
    controller: MachineController,
) -> None:
    command = deserialize_command(message_value)

    execution_result = controller.execute_command(
        command
    )

    command_result = create_command_result(
        command=command,
        execution_result=execution_result,
    )

    publish_result(
        producer=producer,
        command_result=command_result,
    )

    producer.flush(10)

    print(
        f"Macchina={command['machine_id']} "
        f"azione={command['action']} "
        f"esecuzione="
        f"{execution_result['execution_number']} "
        f"risultato="
        f"{execution_result['result']} "
        f"correlation_id="
        f"{command['correlation_id']}",
        flush=True,
    )


def handle_shutdown(
    signum,
    frame,
) -> None:
    del signum, frame

    global running
    running = False

    print(
        "Arresto del Machine Controller richiesto",
        flush=True,
    )


def run_controller() -> None:
    consumer = create_consumer()
    producer = create_producer()

    controller = MachineController(
        mode=CONTROLLER_MODE
    )

    consumer.subscribe([COMMANDS_TOPIC])

    print(
        "Machine Controller avviato: "
        f"{CONTROLLER_ID}",
        flush=True,
    )
    print(
        f"Broker: {KAFKA_BROKER}",
        flush=True,
    )
    print(
        f"Topic comandi: {COMMANDS_TOPIC}",
        flush=True,
    )
    print(
        "Topic risultati: "
        f"{COMMAND_RESULTS_TOPIC}",
        flush=True,
    )
    print(
        f"Modalità Controller: {CONTROLLER_MODE}",
        flush=True,
    )

    try:
        while running:
            message = consumer.poll(
                timeout=1.0
            )

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
                process_command(
                    message_value=message.value(),
                    producer=producer,
                    controller=controller,
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
                    "Comando non elaborabile: "
                    f"{error}",
                    flush=True,
                )

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

    finally:
        producer.flush(10)
        consumer.close()

        print(
            "Machine Controller arrestato "
            "correttamente",
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

    run_controller()


if __name__ == "__main__":
    main()