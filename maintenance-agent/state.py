from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MachineState:
    machine_id: str
    window_size: int
    temperatures: deque[float] = field(init=False)
    vibrations: deque[float] = field(init=False)
    last_action: str = "NO_ACTION"
    last_command_id: str | None = None
    last_correlation_id: str | None = None
    last_command_result: str | None = None
    machine_status: str = "UNKNOWN"

    def __post_init__(self) -> None:
        self.temperatures = deque(
            maxlen=self.window_size
        )
        self.vibrations = deque(
            maxlen=self.window_size
        )

    def update_telemetry(
        self,
        telemetry: dict[str, Any],
    ) -> None:
        self.temperatures.append(
            float(telemetry["temperature"])
        )
        self.vibrations.append(
            float(telemetry["vibration"])
        )

    def update_command_result(
        self,
        command_result: dict[str, Any],
    ) -> None:
        self.last_command_id = command_result[
            "command_id"
        ]
        self.last_correlation_id = command_result[
            "correlation_id"
        ]
        self.last_command_result = command_result[
            "result"
        ]
        self.machine_status = command_result.get(
            "machine_status",
            self.machine_status,
        )

    def average_temperature(self) -> float:
        return self._average(self.temperatures)

    def average_vibration(self) -> float:
        return self._average(self.vibrations)

    def temperature_is_rising(self) -> bool:
        return self._is_rising(self.temperatures)

    def vibration_is_rising(self) -> bool:
        return self._is_rising(self.vibrations)

    def has_enough_history(self) -> bool:
        return len(self.temperatures) >= 3

    @staticmethod
    def _average(
        values: deque[float],
    ) -> float:
        if not values:
            return 0.0

        return sum(values) / len(values)

    @staticmethod
    def _is_rising(
        values: deque[float],
    ) -> bool:
        if len(values) < 3:
            return False

        recent_values = list(values)[-3:]

        return (
            recent_values[0]
            < recent_values[1]
            < recent_values[2]
        )