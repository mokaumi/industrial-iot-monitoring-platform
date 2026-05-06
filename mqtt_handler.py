import json
import base64
import paho.mqtt.client as mqtt

from database import insert_data
from decoders import decode_payload, decode_temperature_payload, decode_smoke_payload


def on_mqtt_message(client, userdata, msg):
    try:
        message = msg.payload.decode()
        print("Raw MQTT:", message)

        packet = json.loads(message)

        site = packet.get("site", "UNKNOWN_SITE")
        device_name = packet.get("device_name", "UNKNOWN_DEVICE")
        device_type = packet.get("device_type", "unknown")
        device_eui = packet.get("device_eui", "UNKNOWN_EUI")
        payload = packet.get("payload")

        if payload is None:
            print("No payload found")
            return

        if device_type in ["ac_meter_generator", "ac_meter_grid"]:
            payload += "=" * (-len(payload) % 4)
            raw_bytes = base64.b64decode(payload)
            hex_payload = raw_bytes.hex()
            print("HEX:", hex_payload)
            decoded = decode_payload(hex_payload)

        elif device_type == "temperature_sensor":
            decoded = decode_temperature_payload(payload)

        elif device_type == "smoke_detector":
            decoded = decode_smoke_payload(payload)

        else:
            decoded = {"raw_payload": payload}

        print("Decoded:", decoded)

        insert_data(
            site,
            device_name,
            device_type,
            device_eui,
            "MQTT",
            None,
            None,
            str(decoded),
            None
        )

    except Exception as e:
        print("MQTT error:", e)


def mqtt_listener():
    client = mqtt.Client()
    client.on_message = on_mqtt_message

    client.connect("localhost", 1883, 60)
    client.subscribe("sensors/telemetry")

    client.loop_forever()