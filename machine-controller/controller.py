from dataclasses import dataclass


REDUCE_SPEED = "REDUCE_SPEED"
REQUEST_INSPECTION = "REQUEST_INSPECTION"
EMERGENCY_STOP = "EMERGENCY_STOP"

SUCCESS = "SUCCESS"
FAILED = "FAILED"


@dataclass
class MachineRuntimeState:
    speed: int = 1400
    status: str = "RUNNING"
    inspection_requested: bool = False


class MachineController:
    def __init__(self) -> None:
        self.machine_states: dict[
            str,
            MachineRuntimeState,
        ] = {}

    def execute_command(self, command: dict) -> dict:
        machine_id = command["machine_id"]
        action = command["action"]

        machine_state = self._get_machine_state(
            machine_id
        )

        if action == REDUCE_SPEED:
            return self._reduce_speed(machine_state)

        if action == REQUEST_INSPECTION:
            return self._request_inspection(
                machine_state
            )

        if action == EMERGENCY_STOP:
            return self._emergency_stop(
                machine_state
            )

        return self._create_unsupported_result(
            action,
            machine_state,
        )

    def _get_machine_state(
        self,
        machine_id: str,
    ) -> MachineRuntimeState:
        if machine_id not in self.machine_states:
            self.machine_states[
                machine_id
            ] = MachineRuntimeState()

        return self.machine_states[machine_id]

    @staticmethod
    def _reduce_speed(
        machine_state: MachineRuntimeState,
    ) -> dict:
        previous_speed = machine_state.speed

        machine_state.speed = 900
        machine_state.status = "REDUCED_SPEED"

        return {
            "result": SUCCESS,
            "message": "Machine speed reduced",
            "machine_status": machine_state.status,
            "previous_speed": previous_speed,
            "current_speed": machine_state.speed,
            "inspection_requested": (
                machine_state.inspection_requested
            ),
        }

    @staticmethod
    def _request_inspection(
        machine_state: MachineRuntimeState,
    ) -> dict:
        machine_state.inspection_requested = True
        machine_state.status = "INSPECTION_REQUIRED"

        return {
            "result": SUCCESS,
            "message": (
                "Maintenance inspection requested"
            ),
            "machine_status": machine_state.status,
            "current_speed": machine_state.speed,
            "inspection_requested": True,
        }

    @staticmethod
    def _emergency_stop(
        machine_state: MachineRuntimeState,
    ) -> dict:
        previous_speed = machine_state.speed

        machine_state.speed = 0
        machine_state.status = "STOPPED"

        return {
            "result": SUCCESS,
            "message": "Emergency stop completed",
            "machine_status": machine_state.status,
            "previous_speed": previous_speed,
            "current_speed": machine_state.speed,
            "inspection_requested": (
                machine_state.inspection_requested
            ),
        }

    @staticmethod
    def _create_unsupported_result(
        action: str,
        machine_state: MachineRuntimeState,
    ) -> dict:
        return {
            "result": FAILED,
            "message": f"Unsupported action: {action}",
            "machine_status": machine_state.status,
            "current_speed": machine_state.speed,
            "inspection_requested": (
                machine_state.inspection_requested
            ),
        }