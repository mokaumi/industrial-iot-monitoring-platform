import json


def decode_payload(raw_payload, device_type=None):
    """
    Converts raw payload into normalized telemetry dictionary.
    Supports:
    - JSON string payload
    - Python dict-like string payload
    - HEX payload fallback
    """

    if raw_payload is None:
        return {}

    # If already dictionary
    if isinstance(raw_payload, dict):
        return raw_payload

    raw_payload = str(raw_payload).strip()

    # Try JSON
    try:
        return json.loads(raw_payload)
    except Exception:
        pass

    # Try Python dict string saved like {'temperature': 72}
    try:
        fixed = raw_payload.replace("'", '"')
        return json.loads(fixed)
    except Exception:
        pass

    # Try HEX to ASCII
    try:
        ascii_text = bytes.fromhex(raw_payload).decode(errors="ignore")
        return {
            "raw_hex": raw_payload,
            "ascii": ascii_text
        }
    except Exception:
        pass

    # Fallback
    return {
        "raw_payload": raw_payload
    }
