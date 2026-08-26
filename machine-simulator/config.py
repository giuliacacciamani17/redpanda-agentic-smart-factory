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

SIMULATION_MODE = os.getenv(
    "SIMULATION_MODE",
    "NORMAL",
).upper()