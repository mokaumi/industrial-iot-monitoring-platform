from payload_decoder import decode_payload
import json
import time
import paho.mqtt.client as mqtt

from database import insert_data


BROKER = "localhost"
PORT = 1883
TOPIC = "iot/telemetry"
NORMALIZED_TOPIC = "iot/normalized"


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker:", rc)
    client.subscribe(TOPIC)
    print("Subscribed to:", TOPIC)


def on_message(client, userdata, msg):

    try:
        raw_payload = msg.payload.decode()

        print("RAW MQTT:", raw_payload)

        decoded_payload = decode_payload(raw_payload)

        print("DECODED PAYLOAD:", decoded_payload)

        # If payload is not telemetry JSON, stop here
        if "temperature" not in decoded_payload:
            print("Non-telemetry payload received")
            return
        decoded_payload.setdefault("event", 1)
        decoded_payload.setdefault("state", 1)
        decoded_payload.setdefault("movement", "MQTT")
        decoded_payload.setdefault("battery", 3.8)

        insert_data(
            decoded_payload.get("site", "UNKNOWN_SITE"),
            decoded_payload.get("device_name", "MQTT Device"),
            decoded_payload.get("device_type", "temperature_sensor"),
            decoded_payload.get("device_eui", "UNKNOWN_EUI"),
            decoded_payload.get("freq", "MQTT"),
            decoded_payload.get("rssi", 0),
            decoded_payload.get("snr", 0),
            str(decoded_payload),
            decoded_payload.get("temperature")
        )
        
        
        


        client.publish(
                NORMALIZED_TOPIC,
                json.dumps(decoded_payload)
        )

        print("Published normalized telemetry")


        print("Inserted MQTT data into sensor_data")

    except Exception as e:
        print("MQTT insert error:", e)


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

print("Starting MQTT subscriber...")
client.connect(BROKER, PORT, 60)
client.loop_forever()