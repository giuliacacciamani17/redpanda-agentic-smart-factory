from dataclasses import dataclass


REDUCE_SPEED = "REDUCE_SPEED"
REQUEST_INSPECTION = "REQUEST_INSPECTION"
EMERGENCY_STOP = "EMERGENCY_STOP"

SUCCESS = "SUCCESS"
FAILED = "FAILED"

MIXED = "MIXED"
ALWAYS_SUCCESS = "ALWAYS_SUCCESS"


@dataclass
class MachineRuntimeState:
    speed: int = 1400
    status: str = "RUNNING"
    inspection_requested: bool = False


class MachineController:
    def __init__(
        self,
        mode: str = MIXED,
    ) -> None:
        self.mode = mode

        self.machine_states: dict[
            str,
            MachineRuntimeState,
        ] = {}

        self.action_counters: dict[str, int] = {
            REDUCE_SPEED: 0,
            REQUEST_INSPECTION: 0,
            EMERGENCY_STOP: 0,
        }

    def execute_command(self, command: dict) -> dict:
        machine_id = command["machine_id"]
        action = command["action"]

        machine_state = self._get_machine_state(
            machine_id
        )

        execution_number = self._next_execution_number(
            action
        )

        if self._should_fail(
            action=action,
            execution_number=execution_number,
        ):
            return self._create_failed_result(
                action=action,
                execution_number=execution_number,
                machine_state=machine_state,
            )

        if action == REDUCE_SPEED:
            return self._reduce_speed(
                machine_state=machine_state,
                execution_number=execution_number,
            )

        if action == REQUEST_INSPECTION:
            return self._request_inspection(
                machine_state=machine_state,
                execution_number=execution_number,
            )

        if action == EMERGENCY_STOP:
            return self._emergency_stop(
                machine_state=machine_state,
                execution_number=execution_number,
            )

        return self._create_unsupported_result(
            action=action,
            execution_number=execution_number,
            machine_state=machine_state,
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

    def _next_execution_number(
        self,
        action: str,
    ) -> int:
        current_value = self.action_counters.get(
            action,
            0,
        )

        execution_number = current_value + 1
        self.action_counters[action] = execution_number

        return execution_number

    def _should_fail(
        self,
        action: str,
        execution_number: int,
    ) -> bool:
        if self.mode == ALWAYS_SUCCESS:
            return False

        if self.mode != MIXED:
            return False

        failure_sequences = {
            REDUCE_SPEED: {1},
            REQUEST_INSPECTION: set(),
            EMERGENCY_STOP: {1},
        }

        return execution_number in failure_sequences.get(
            action,
            set(),
        )

    @staticmethod
    def _reduce_speed(
        machine_state: MachineRuntimeState,
        execution_number: int,
    ) -> dict:
        previous_speed = machine_state.speed

        machine_state.speed = 900
        machine_state.status = "REDUCED_SPEED"

        return {
            "result": SUCCESS,
            "message": "Machine speed reduced",
            "failure_reason": None,
            "execution_number": execution_number,
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
        execution_number: int,
    ) -> dict:
        machine_state.inspection_requested = True
        machine_state.status = "INSPECTION_REQUIRED"

        return {
            "result": SUCCESS,
            "message": (
                "Maintenance inspection requested"
            ),
            "failure_reason": None,
            "execution_number": execution_number,
            "machine_status": machine_state.status,
            "previous_speed": machine_state.speed,
            "current_speed": machine_state.speed,
            "inspection_requested": True,
        }

    @staticmethod
    def _emergency_stop(
        machine_state: MachineRuntimeState,
        execution_number: int,
    ) -> dict:
        previous_speed = machine_state.speed

        machine_state.speed = 0
        machine_state.status = "STOPPED"

        return {
            "result": SUCCESS,
            "message": "Emergency stop completed",
            "failure_reason": None,
            "execution_number": execution_number,
            "machine_status": machine_state.status,
            "previous_speed": previous_speed,
            "current_speed": machine_state.speed,
            "inspection_requested": (
                machine_state.inspection_requested
            ),
        }

    @staticmethod
    def _create_failed_result(
        action: str,
        execution_number: int,
        machine_state: MachineRuntimeState,
    ) -> dict:
        return {
            "result": FAILED,
            "message": (
                f"Command execution failed: {action}"
            ),
            "failure_reason": (
                "Simulated actuator communication failure"
            ),
            "execution_number": execution_number,
            "machine_status": machine_state.status,
            "previous_speed": machine_state.speed,
            "current_speed": machine_state.speed,
            "inspection_requested": (
                machine_state.inspection_requested
            ),
        }

    @staticmethod
    def _create_unsupported_result(
        action: str,
        execution_number: int,
        machine_state: MachineRuntimeState,
    ) -> dict:
        return {
            "result": FAILED,
            "message": f"Unsupported action: {action}",
            "failure_reason": "Unsupported command",
            "execution_number": execution_number,
            "machine_status": machine_state.status,
            "previous_speed": machine_state.speed,
            "current_speed": machine_state.speed,
            "inspection_requested": (
                machine_state.inspection_requested
            ),
        }