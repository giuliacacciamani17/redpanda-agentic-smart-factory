import os


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "factory.telemetry")
COMMANDS_TOPIC = os.getenv("COMMANDS_TOPIC", "factory.commands")
DECISIONS_TOPIC = os.getenv(
    "DECISIONS_TOPIC",
    "factory.agent-decisions",
)

CONSUMER_GROUP = os.getenv(
    "CONSUMER_GROUP",
    "maintenance-agent-group",
)

AGENT_ID = os.getenv(
    "AGENT_ID",
    "maintenance-agent-01",
)

STATE_WINDOW_SIZE = int(
    os.getenv("STATE_WINDOW_SIZE", "5")
)