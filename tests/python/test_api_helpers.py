import asyncio

import main


def test_health_endpoint_payload() -> None:
    assert asyncio.run(main.health()) == {"status": "ok"}


def test_coerce_alert_record_normalizes_risk_and_alert_flag() -> None:
    record = {
        "vehicle_id": "vehicle-1",
        "delay_increase_risk": "0.75",
        "delay_increase_alert": 1,
    }

    coerced = main._coerce_alert_record(record)

    assert coerced is not None
    assert coerced["delay_increase_risk"] == 0.75
    assert coerced["delay_increase_alert"] is True


def test_coerce_alert_record_defaults_invalid_risk() -> None:
    coerced = main._coerce_alert_record(
        {
            "vehicle_id": "vehicle-1",
            "delay_increase_risk": "not-a-number",
            "delay_increase_alert": False,
        }
    )

    assert coerced is not None
    assert coerced["delay_increase_risk"] == 0.0
    assert coerced["delay_increase_alert"] is False


def test_coerce_alert_record_rejects_non_mapping_values() -> None:
    assert main._coerce_alert_record(["not", "a", "record"]) is None
