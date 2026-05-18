import json
import time
import math
import random
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC = "iot/telemetry"

client = mqtt.Client()
client.connect(BROKER, PORT, 60)

counter = 0

print("MQTT device simulator started...")

while True:

    # Generator
    gen_temp = 65 + (15 * math.sin(counter / 5))
    gen_humidity = 35 + (8 * math.sin(counter / 6))

    generator_payload = {
        "site": "Abia",
        "device_name": "MQTT Generator Sensor",
        "device_type": "temperature_sensor",
        "device_eui": "MQTT_GEN_001",
        "temperature": round(gen_temp, 1),
        "humidity": round(gen_humidity, 1),
        "battery": 3.9,
        "event": 1,
        "state": 1
    }

    # Warehouse
    warehouse_temp = 22 + (5 * math.sin(counter / 7))
    warehouse_humidity = 55 + (10 * math.sin(counter / 8))

    warehouse_payload = {
        "site": "Abia",
        "device_name": "MQTT Warehouse Sensor",
        "device_type": "temperature_sensor",
        "device_eui": "MQTT_WAREHOUSE_001",
        "temperature": round(warehouse_temp, 1),
        "humidity": round(warehouse_humidity, 1),
        "battery": 3.8,
        "event": 1,
        "state": 1
    }

    # Cold room
    cold_temp = 5 + (2 * math.sin(counter / 4))
    cold_humidity = 75 + (5 * math.sin(counter / 5))

    cold_payload = {
        "site": "Abia",
        "device_name": "MQTT Cold Room Sensor",
        "device_type": "temperature_sensor",
        "device_eui": "MQTT_COLD_001",
        "temperature": round(cold_temp, 1),
        "humidity": round(cold_humidity, 1),
        "battery": 3.7,
        "event": 1,
        "state": 1
    }

    devices = [
        generator_payload,
        warehouse_payload,
        cold_payload
    ]

    for payload in devices:
        client.publish(TOPIC, json.dumps(payload))
        print("Published:", payload)

    print("-" * 50)

    counter += 1
    time.sleep(5)
