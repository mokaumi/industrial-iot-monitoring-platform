from fastapi import FastAPI, WebSocket
import asyncio
import sqlite3
import json
import paho.mqtt.client as mqtt

app = FastAPI()
connected_clients = []




latest_message = {}

def on_mqtt_message(client, userdata, msg):
    global latest_message

    try:
        payload = msg.payload.decode()
        latest_message = json.loads(payload)
        print("FastAPI received normalized MQTT:", latest_message)

    except Exception as e:
        print("FastAPI MQTT error:", e)





@app.get("/")
def home():
    return {"message": "FastAPI WebSocket server running"}


@app.websocket("/ws/telemetry")
async def telemetry_ws(websocket: WebSocket):
    await websocket.accept()

    try:
        last_sent = None

        while True:
            if latest_message and latest_message != last_sent:
                await websocket.send_text(json.dumps(latest_message))
                last_sent = latest_message.copy()

            await asyncio.sleep(0.1)

    except Exception as e:
        print("WebSocket disconnected:", e)








mqtt_client = mqtt.Client()
mqtt_client.on_message = on_mqtt_message
mqtt_client.connect("localhost", 1883, 60)
mqtt_client.subscribe("iot/normalized")
mqtt_client.loop_start()