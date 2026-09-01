import os


KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "localhost:19092",
)

COMMANDS_TOPIC = os.getenv(
    "COMMANDS_TOPIC",
    "factory.commands",
)

COMMAND_RESULTS_TOPIC = os.getenv(
    "COMMAND_RESULTS_TOPIC",
    "factory.command-results",
)

CONSUMER_GROUP = os.getenv(
    "CONSUMER_GROUP",
    "machine-controller-group",
)

CONTROLLER_ID = os.getenv(
    "CONTROLLER_ID",
    "machine-controller-01",
)

CONTROLLER_MODE = os.getenv(
    "CONTROLLER_MODE",
    "MIXED",
).upper()