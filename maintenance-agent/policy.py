NO_ACTION = "NO_ACTION"
MONITOR = "MONITOR"
REDUCE_SPEED = "REDUCE_SPEED"
REQUEST_INSPECTION = "REQUEST_INSPECTION"
EMERGENCY_STOP = "EMERGENCY_STOP"


def select_action(risk_score: float) -> str:
    if risk_score >= 0.85:
        return EMERGENCY_STOP

    if risk_score >= 0.65:
        return REQUEST_INSPECTION

    if risk_score >= 0.45:
        return REDUCE_SPEED

    if risk_score >= 0.20:
        return MONITOR

    return NO_ACTION


def explain_action(action: str, risk_score: float) -> str:
    explanations = {
        NO_ACTION: "Operating conditions are within normal limits",
        MONITOR: "A weak anomaly requires additional monitoring",
        REDUCE_SPEED: "The risk level requires a speed reduction",
        REQUEST_INSPECTION: (
            "The risk level requires a maintenance inspection"
        ),
        EMERGENCY_STOP: (
            "The risk level requires an emergency stop"
        ),
    }

    explanation = explanations[action]

    return f"{explanation}. Risk score: {risk_score:.2f}"
