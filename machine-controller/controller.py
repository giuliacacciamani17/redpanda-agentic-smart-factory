REDUCE_SPEED = "REDUCE_SPEED"
REQUEST_INSPECTION = "REQUEST_INSPECTION"
EMERGENCY_STOP = "EMERGENCY_STOP"

SUCCESS = "SUCCESS"
FAILED = "FAILED"

MIXED = "MIXED"
ALWAYS_SUCCESS = "ALWAYS_SUCCESS"


class MachineController:
    def __init__(
        self,
        mode: str = MIXED,
    ) -> None:
        self.mode = mode

        self.action_counters: dict[str, int] = {
            REDUCE_SPEED: 0,
            REQUEST_INSPECTION: 0,
            EMERGENCY_STOP: 0,
        }

    def execute_command(
        self,
        command: dict,
    ) -> dict:
        action = command["action"]

        execution_number = (
            self._next_execution_number(action)
        )

        if self._should_fail(
            action=action,
            execution_number=execution_number,
        ):
            return self._create_failed_result(
                action=action,
                execution_number=execution_number,
            )

        if action == REDUCE_SPEED:
            return self._create_success_result(
                action=action,
                execution_number=execution_number,
                message=(
                    "Speed reduction command "
                    "executed successfully"
                ),
            )

        if action == REQUEST_INSPECTION:
            return self._create_success_result(
                action=action,
                execution_number=execution_number,
                message=(
                    "Maintenance inspection request "
                    "executed successfully"
                ),
            )

        if action == EMERGENCY_STOP:
            return self._create_success_result(
                action=action,
                execution_number=execution_number,
                message=(
                    "Emergency stop command "
                    "executed successfully"
                ),
            )

        return self._create_unsupported_result(
            action=action,
            execution_number=execution_number,
        )

    def _next_execution_number(
        self,
        action: str,
    ) -> int:
        current_value = self.action_counters.get(
            action,
            0,
        )

        execution_number = current_value + 1

        self.action_counters[action] = (
            execution_number
        )

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

        failed_executions = failure_sequences.get(
            action,
            set(),
        )

        return execution_number in failed_executions

    @staticmethod
    def _create_success_result(
        action: str,
        execution_number: int,
        message: str,
    ) -> dict:
        return {
            "result": SUCCESS,
            "message": message,
            "failure_reason": None,
            "execution_number": execution_number,
            "executed_action": action,
        }

    @staticmethod
    def _create_failed_result(
        action: str,
        execution_number: int,
    ) -> dict:
        return {
            "result": FAILED,
            "message": (
                f"Command execution failed: {action}"
            ),
            "failure_reason": (
                "Simulated actuator communication "
                "failure"
            ),
            "execution_number": execution_number,
            "executed_action": action,
        }

    @staticmethod
    def _create_unsupported_result(
        action: str,
        execution_number: int,
    ) -> dict:
        return {
            "result": FAILED,
            "message": (
                f"Unsupported action: {action}"
            ),
            "failure_reason": "Unsupported command",
            "execution_number": execution_number,
            "executed_action": action,
        }