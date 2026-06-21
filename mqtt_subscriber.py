from payload_decoder import decode_payload
import json
import time
import paho.mqtt.client as mqtt

# from database import insert_data
from postgres_db import (
    insert_sensor_data_pg,
    evaluate_alarm_rules_pg,
    insert_anomaly_event_pg,
    create_or_update_active_alarm_pg,
    clear_active_alarm_pg,
    recent_anomaly_exists_pg,
    insert_command_response_pg,
    update_reported_twin_pg,
    insert_gateway_telemetry_pg,
    insert_gateway_command_response_pg
)

# from database import get_asset_by_device

BROKER = "mosquitto"
PORT = 1883
TOPIC = "iot/telemetry"
NORMALIZED_TOPIC = "iot/normalized"
COMMAND_RESPONSE_TOPIC = "devices/command_responses"
TWIN_REPORTED_TOPIC = "devices/twin_reported"
GATEWAY_TELEMETRY_TOPIC = "gateways/telemetry"
GATEWAY_COMMAND_RESPONSE_TOPIC = "gateways/command_responses"


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with result code", rc)

    client.subscribe(TOPIC)
    client.subscribe(COMMAND_RESPONSE_TOPIC)
    client.subscribe(TWIN_REPORTED_TOPIC)
    client.subscribe(GATEWAY_TELEMETRY_TOPIC)
    client.subscribe(GATEWAY_COMMAND_RESPONSE_TOPIC)
    
    
    print("Subscribed to:", GATEWAY_COMMAND_RESPONSE_TOPIC)
    print("Subscribed to:", GATEWAY_TELEMETRY_TOPIC)
    print("Subscribed to:", TOPIC)
    print("Subscribed to:", COMMAND_RESPONSE_TOPIC)
    print("Subscribed to:", TWIN_REPORTED_TOPIC)


def on_message(client, userdata, msg):

    try:
        raw_payload = msg.payload.decode()
        
        if msg.topic == GATEWAY_COMMAND_RESPONSE_TOPIC:
            data = json.loads(raw_payload)

            insert_gateway_command_response_pg(
                data.get("gateway_eui"),
                data.get("command"),
                data.get("status"),
                data.get("message"),
                data.get("source")
            )

            print("Inserted gateway command response into PostgreSQL")
            return
        
        if msg.topic == GATEWAY_TELEMETRY_TOPIC:
            data = json.loads(raw_payload)

            insert_gateway_telemetry_pg(
                data.get("gateway_eui"),
                data.get("cpu_usage"),
                data.get("memory_usage"),
                data.get("signal_quality"),
                data.get("packets_today"),
                data.get("status")
            )

            print("Inserted gateway telemetry into PostgreSQL")
            return

        print("RAW MQTT:", raw_payload)
        
        if msg.topic == TWIN_REPORTED_TOPIC:
            reported = json.loads(raw_payload)

            update_reported_twin_pg(
                reported.get("device_eui"),
                reported.get("reporting_interval"),
                reported.get("alarm_enabled")
            )

            print("Updated reported twin:", reported)
            return
        
        if msg.topic == COMMAND_RESPONSE_TOPIC:
            response = json.loads(raw_payload)

            insert_command_response_pg(
                response.get("device_eui"),
                response.get("command"),
                response.get("status"),
                response.get("message"),
                response.get("source")
            )

            print("Inserted command response into PostgreSQL")
            return

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

        # insert_data(
        #     decoded_payload.get("site", "UNKNOWN_SITE"),
        #     decoded_payload.get("device_name", "MQTT Device"),
        #     decoded_payload.get("device_type", "temperature_sensor"),
        #     decoded_payload.get("device_eui", "UNKNOWN_EUI"),
        #     decoded_payload.get("freq", "MQTT"),
        #     decoded_payload.get("rssi", 0),
        #     decoded_payload.get("snr", 0),
        #     str(decoded_payload),
        #     decoded_payload.get("temperature")
        # )
        
        
        
        

        asset_id = 1 if decoded_payload.get("device_eui") == "MQTT_GEN_001" else None
        asset_name = "Generator" if decoded_payload.get("device_eui") == "MQTT_GEN_001" else None
        asset_type = "AC METER" if decoded_payload.get("device_eui") == "MQTT_GEN_001" else None

        try:
            insert_sensor_data_pg(
                decoded_payload.get("site", "UNKNOWN_SITE"),
                decoded_payload.get("device_name", "MQTT Device"),
                decoded_payload.get("device_type", "temperature_sensor"),
                decoded_payload.get("device_eui", "UNKNOWN_EUI"),
                decoded_payload.get("freq", "MQTT"),
                decoded_payload.get("rssi", 0),
                decoded_payload.get("snr", 0),
                str(decoded_payload),
                decoded_payload.get("temperature"),
                asset_id,
                asset_name,
                asset_type
            )

            print("Inserted MQTT data into PostgreSQL")
            alarms = evaluate_alarm_rules_pg(
                decoded_payload.get("device_eui"),
                decoded_payload
            )

            triggered_parameters = []

            for alarm in alarms:
                triggered_parameters.append(alarm["parameter"])

                create_or_update_active_alarm_pg(
                    decoded_payload.get("device_eui"),
                    alarm["parameter"],
                    alarm["reason"],
                    alarm["severity"]
                )

                if not recent_anomaly_exists_pg(
                    decoded_payload.get("device_eui"),
                    "HIGH" if alarm["severity"] == "CRITICAL" else "MEDIUM",
                    minutes=5
                ):
                    insert_anomaly_event_pg(
                        decoded_payload.get("site", "UNKNOWN_SITE"),
                        decoded_payload.get("device_eui"),
                        decoded_payload.get("device_type", "unknown"),
                        100 if alarm["severity"] == "CRITICAL" else 70,
                        "HIGH" if alarm["severity"] == "CRITICAL" else "MEDIUM",
                        alarm["reason"]
                    )

                print("Active alarm updated:", alarm["reason"])

                # Clear active alarms when value returns to normal
            for parameter in ["temperature", "humidity", "battery", "pressure", "smoke"]:
                if parameter in decoded_payload and parameter not in triggered_parameters:
                    clear_active_alarm_pg(
                        decoded_payload.get("device_eui"),
                        parameter
                    )

        except Exception as pg_error:
            print("PostgreSQL insert error:", pg_error)
                  
        
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
