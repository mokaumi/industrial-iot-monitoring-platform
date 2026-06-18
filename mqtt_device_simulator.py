import json
import time
import math
import random
import paho.mqtt.client as mqtt

BROKER = "mosquitto"
PORT = 1883
TOPIC = "iot/telemetry"
COMMAND_TOPIC = "devices/+/commands"
RESPONSE_TOPIC = "devices/command_responses"


def on_command(client, userdata, msg):
    try:
        raw = msg.payload.decode()
        command_data = json.loads(raw)

        print("COMMAND RECEIVED:", msg.topic, command_data)
        
        if command_data.get("command") == "update_config":

            response = {
                "device_eui": command_data.get("device_eui"),
                "reporting_interval": command_data.get("reporting_interval"),
                "alarm_enabled": command_data.get("alarm_enabled"),
                "source": "device_simulator",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            client.publish("devices/twin_reported", json.dumps(response))

            print("TWIN REPORTED SENT:", response)

            return

        response = {
            "device_eui": command_data.get("device_eui"),
            "command": command_data.get("command"),
            "status": "SUCCESS",
            "message": f"{command_data.get('command')} executed successfully",
            "source": "device_simulator",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        client.publish(RESPONSE_TOPIC, json.dumps(response))

        print("COMMAND RESPONSE SENT:", response)

    except Exception as e:
        print("Command handling error:", e)


client = mqtt.Client()

client.on_message = on_command

client.connect(BROKER, PORT, 60)

client.subscribe(COMMAND_TOPIC)

print("Subscribed to command topic:", COMMAND_TOPIC)

client.loop_start()

counter = 0

print("MQTT device simulator started...")
while True:

    gen_temp = 65 + (15 * math.sin(counter / 5))
    gen_humidity = 35 + (8 * math.sin(counter / 6))

    warehouse_temp = 22 + (5 * math.sin(counter / 7))
    warehouse_humidity = 55 + (10 * math.sin(counter / 8))

    cold_temp = 5 + (2 * math.sin(counter / 4))
    cold_humidity = 75 + (5 * math.sin(counter / 5))

    pressure = 45 + (20 * math.sin(counter / 6))
    smoke_value = random.choice([0, 0, 0, 0, 1])

    dc_voltage = 52 + (3 * math.sin(counter / 5))
    dc_current = 15 + (5 * math.sin(counter / 4))

    grid_voltage = 230 + (8 * math.sin(counter / 7))
    grid_current = 12 + (4 * math.sin(counter / 6))

    devices = [
        {
            "site": "Abia",
            "device_name": "MQTT Generator Sensor",
            "device_type": "temperature_sensor",
            "device_eui": "MQTT_GEN_001",
            "temperature": round(gen_temp, 1),
            "humidity": round(gen_humidity, 1),
            "battery": 3.9,
            "event": 1,
            "state": 1
        },
        {
            "site": "Abia",
            "device_name": "MQTT Warehouse Sensor",
            "device_type": "temperature_sensor",
            "device_eui": "MQTT_WAREHOUSE_001",
            "temperature": round(warehouse_temp, 1),
            "humidity": round(warehouse_humidity, 1),
            "battery": 3.8,
            "event": 1,
            "state": 1
        },
        {
            "site": "Abia",
            "device_name": "MQTT Cold Room Sensor",
            "device_type": "temperature_sensor",
            "device_eui": "MQTT_COLD_001",
            "temperature": round(cold_temp, 1),
            "humidity": round(cold_humidity, 1),
            "battery": 3.7,
            "event": 1,
            "state": 1
        },
        {
            "site": "BORNO",
            "device_name": "PRESSURE_SENSOR",
            "device_type": "FUEL_sensor",
            "device_eui": "09876543134",
            "pressure": round(pressure, 1),
            "temperature": round(30 + (5 * math.sin(counter / 5)), 1),
            "battery": 3.8,
            "event": 1,
            "state": 1
        },
        {
            "site": "LAGOS",
            "device_name": "SMOKE_DETECTOR",
            "device_type": "Smoke_sensor",
            "device_eui": "7409871345",
            "smoke": smoke_value,
            "temperature": round(28 + (8 * math.sin(counter / 6)), 1),
            "battery": 3.9,
            "event": smoke_value,
            "state": 1
        },
        {
            "site": "Abia",
            "device_name": "DC_RECTIFIER",
            "device_type": "DC_METER",
            "device_eui": "631246789023",
            "voltage": round(dc_voltage, 1),
            "current": round(dc_current, 1),
            "power": round(dc_voltage * dc_current / 1000, 2),
            "temperature": round(35 + (5 * math.sin(counter / 7)), 1),
            "battery": 3.9,
            "event": 1,
            "state": 1
        },
        {
            "site": "Abia",
            "device_name": "GRID_METER",
            "device_type": "ac_meter",
            "device_eui": "12456789876554",
            "voltage": round(grid_voltage, 1),
            "current": round(grid_current, 1),
            "power": round(grid_voltage * grid_current / 1000, 2),
            "temperature": round(32 + (4 * math.sin(counter / 8)), 1),
            "battery": 3.9,
            "event": 1,
            "state": 1
        }
    ]

    for payload in devices:
        client.publish(TOPIC, json.dumps(payload))
        print("Published:", payload)

    print("-" * 50)

    counter += 1
    time.sleep(1)