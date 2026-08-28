import os


KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "localhost:19092",
)

TELEMETRY_TOPIC = os.getenv(
    "TELEMETRY_TOPIC",
    "factory.telemetry",
)

MACHINE_ID = os.getenv(
    "MACHINE_ID",
    "machine-01",
)

EVENT_INTERVAL_SECONDS = float(
    os.getenv("EVENT_INTERVAL_SECONDS", "2")
)

RANDOM_SEED = int(
    os.getenv("RANDOM_SEED", "42")
)

CORRELATION_PREFIX = os.getenv(
    "CORRELATION_PREFIX",
    "abc",
)

CORRELATION_START = int(
    os.getenv("CORRELATION_START", "123")
)