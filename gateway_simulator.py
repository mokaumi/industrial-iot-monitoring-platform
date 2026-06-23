import json
import time
import random
import paho.mqtt.client as mqtt

BROKER = "mosquitto"
PORT = 1883
TOPIC = "gateways/telemetry"
GATEWAY_COMMAND_TOPIC = "gateways/+/commands"
GATEWAY_RESPONSE_TOPIC = "gateways/command_responses"


def on_gateway_command(client, userdata, msg):
    try:
        raw = msg.payload.decode()
        command_data = json.loads(raw)

        print("GATEWAY COMMAND RECEIVED:", msg.topic, command_data)

        response = {
            "gateway_eui": command_data.get("gateway_eui"),
            "command": command_data.get("command"),
            "status": "SUCCESS",
            "message": f"{command_data.get('command')} executed successfully",
            "source": "gateway_simulator",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        client.publish(GATEWAY_RESPONSE_TOPIC, json.dumps(response))
        print("GATEWAY RESPONSE SENT:", response)

    except Exception as e:
        print("Gateway command error:", e)


client = mqtt.Client()
client.on_message = on_gateway_command
client.connect(BROKER, PORT, 60)
client.subscribe(GATEWAY_COMMAND_TOPIC)

print("Subscribed to:", GATEWAY_COMMAND_TOPIC)

client.loop_start()

print("Gateway simulator started...")

packets_today = 10000
while True:
    packets_today += random.randint(50, 200)

    payload = {
        "gateway_eui": "GW_ABIA_001",
        "cpu_usage": 95,
        "memory_usage": random.randint(30, 70),
        "signal_quality": random.randint(88, 100),
        "status": "ONLINE",
        "packets_today": packets_today
    }

    client.publish(TOPIC, json.dumps(payload))

    print("Published gateway telemetry:", payload)

    time.sleep(10)
