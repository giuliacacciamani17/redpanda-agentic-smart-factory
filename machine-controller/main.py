import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer

from config import (
    COMMAND_RESULTS_TOPIC,
    COMMANDS_TOPIC,
    CONSUMER_GROUP,
    CONTROLLER_ID,
    KAFKA_BROKER,
)
from controller import MachineController


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


def handle_delivery(error, message) -> None:
    if error is not None:
        print(
            f"Errore durante la pubblicazione: {error}",
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


def create_command_result(
    command: dict,
    execution_result: dict,
) -> dict:
    return {
        "result_id": str(uuid.uuid4()),
        "command_id": command["command_id"],
        "decision_id": command.get("decision_id"),
        "controller_id": CONTROLLER_ID,
        "agent_id": command.get("agent_id"),
        "machine_id": command["machine_id"],
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "action": command["action"],
        **execution_result,
    }


def publish_result(
    producer: Producer,
    command_result: dict,
) -> None:
    producer.produce(
        topic=COMMAND_RESULTS_TOPIC,
        key=command_result["machine_id"].encode("utf-8"),
        value=json.dumps(
            command_result
        ).encode("utf-8"),
        callback=handle_delivery,
    )

    producer.poll(0)


def run_controller() -> None:
    consumer = create_consumer()
    producer = create_producer()
    controller = MachineController()

    consumer.subscribe([COMMANDS_TOPIC])

    print(
        f"Machine Controller avviato: {CONTROLLER_ID}",
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
        f"Topic risultati: {COMMAND_RESULTS_TOPIC}",
        flush=True,
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if (
                    message.error().code()
                    == KafkaError._PARTITION_EOF
                ):
                    continue

                print(
                    f"Errore consumer: {message.error()}",
                    flush=True,
                )
                continue

            command = json.loads(
                message.value().decode("utf-8")
            )

            execution_result = (
                controller.execute_command(command)
            )

            command_result = create_command_result(
                command,
                execution_result,
            )

            publish_result(
                producer,
                command_result,
            )

            producer.flush()
            consumer.commit(
                message=message,
                asynchronous=False,
            )

            print(
                f"Macchina={command['machine_id']} "
                f"azione={command['action']} "
                f"risultato={execution_result['result']} "
                f"stato={execution_result['machine_status']}",
                flush=True,
            )

    except KeyboardInterrupt:
        print(
            "Arresto del Machine Controller richiesto",
            flush=True,
        )

    finally:
        producer.flush()
        consumer.close()

        print(
            "Machine Controller arrestato correttamente",
            flush=True,
        )


if __name__ == "__main__":
    run_controller()