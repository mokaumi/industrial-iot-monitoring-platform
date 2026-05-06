import base64


def decode_temperature_payload(base64_payload):
    try:
        base64_payload += "=" * (-len(base64_payload) % 4)
        raw_bytes = base64.b64decode(base64_payload)
        text = raw_bytes.decode("utf-8")

        print("TEMP TEXT:", text)

        parts = text.split(",")

        return {
            "raw": text,
            "device_eui": parts[2] if len(parts) > 2 else None,
            "event": int(parts[5]) if len(parts) > 5 else None,
            "state": int(parts[6]) if len(parts) > 6 else None,
            "temperature": float(parts[7]) if len(parts) > 7 else None,
            "humidity": float(parts[8]) if len(parts) > 8 else None,
            "movement": parts[9] if len(parts) > 9 else None,
            "battery": float(parts[10]) if len(parts) > 10 else None
        }

    except Exception as e:
        print("Temperature decode error:", e)
        return {}


def decode_smoke_payload(base64_payload):
    try:
        base64_payload += "=" * (-len(base64_payload) % 4)
        raw_bytes = base64.b64decode(base64_payload)
        hex_payload = raw_bytes.hex()

        print("SMOKE HEX:", hex_payload)

        alarm_byte = raw_bytes[-1]

        return {
            "raw_hex": hex_payload,
            "fault_alarm": 1 if alarm_byte & 0b00000001 else 0,
            "smoke_alarm": 1 if alarm_byte & 0b00000010 else 0,
            "tamper_alarm": 1 if alarm_byte & 0b00000100 else 0,
            "voltage_alarm": 1 if alarm_byte & 0b00001000 else 0
        }

    except Exception as e:
        print("Smoke decode error:", e)
        return {}


def decode_payload(hex_data):
    result = {}
    i = 0

    while i < len(hex_data):
        try:
            data_id = int(hex_data[i:i+2], 16)
            value_hex = hex_data[i+2:i+10]
            value_bytes = bytes.fromhex(value_hex)
            value = int.from_bytes(value_bytes, byteorder="big")

            if data_id == 0x01:
                result["voltage1"] = value / 10
            elif data_id == 0x02:
                result["voltage2"] = value / 10
            elif data_id == 0x03:
                result["voltage3"] = value / 10
            elif data_id == 0x04:
                result["current1"] = value * 0.3
            elif data_id == 0x05:
                result["current2"] = value * 0.3
            elif data_id == 0x06:
                result["current3"] = value * 0.3
            elif data_id == 0x07:
                result["id_07"] = value
            elif data_id == 0x08:
                result["power1"] = value / 33.3333333333
            elif data_id == 0x09:
                result["power2"] = value / 33.3333333333
            elif data_id == 0x0A:
                result["power3"] = value / 33.3333333333
            elif data_id == 0x0B:
                result["total_power"] = value / 33.3333333333
            elif data_id == 0x0C:
                result["apparent1"] = value / 33.3333333333
            elif data_id == 0x0D:
                result["apparent2"] = value / 33.3333333333
            elif data_id == 0x0E:
                result["apparent3"] = value / 33.3333333333
            elif data_id == 0x0F:
                result["total_apparent"] = value / 33.3333333333
            elif data_id == 0x10:
                result["pf1"] = value / 1000
            elif data_id == 0x11:
                result["pf2"] = value / 1000
            elif data_id == 0x12:
                result["pf3"] = value / 1000
            elif data_id == 0x13:
                result["total_pf"] = value / 1000
            elif data_id == 0x14:
                result["energy"] = value / 50
            elif data_id == 0x15:
                result["frequency"] = value / 100
            elif data_id == 0x16:
                result["id_16"] = value
            else:
                result[f"id_{data_id:02X}"] = value

            i += 10

        except Exception as e:
            print("Decode error:", e)
            break

    return result