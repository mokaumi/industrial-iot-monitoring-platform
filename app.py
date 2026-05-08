from werkzeug.security import generate_password_hash, check_password_hash
from flask import Flask, render_template, jsonify, request, session, redirect, url_for
import paho.mqtt.client as mqtt
import threading
import socket
import json
from datetime import datetime
from database import (
    init_db, insert_data, get_all_data, get_data_by_device, get_devices,
    init_users_table, get_user_by_username, create_user, get_all_users, delete_user_by_id
)
from auth import register_auth_routes, require_role
from admin import register_admin_routes
from decoders import decode_payload, decode_temperature_payload, decode_smoke_payload
from mqtt_handler import mqtt_listener
from udp_handler import udp_listener
import base64
import os
from anomaly import analyze_temperature, analyze_smoke, analyze_ac_meter



app = Flask(__name__)
init_db()
init_users_table()
packets = []
app.secret_key = "change-this-secret-key"
API_KEY = os.getenv("API_KEY", "test123")
register_auth_routes(app)
register_admin_routes(app)






# ---------------- THREADS ----------------
# threading.Thread(target=udp_listener, daemon=True).start()
# threading.Thread(target=mqtt_listener, daemon=True).start()










# ---------------- ROUTES ----------------
@app.route("/api/telemetry", methods=["POST"])
def api_telemetry():
    api_key = request.headers.get("X-API-Key")
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    packet = request.get_json()

    site = packet.get("site", "UNKNOWN_SITE")
    device_name = packet.get("device_name", "UNKNOWN_DEVICE")
    device_type = packet.get("device_type", "unknown")
    device_eui = packet.get("device_eui", "UNKNOWN_EUI")
    payload = packet.get("payload")

    if payload is None:
        return jsonify({"error": "No payload found"}), 400

    if device_type in ["ac_meter_generator", "ac_meter_grid"]:
        payload += "=" * (-len(payload) % 4)
        raw_bytes = base64.b64decode(payload)
        hex_payload = raw_bytes.hex()
        decoded = decode_payload(hex_payload)

    elif device_type == "temperature_sensor":
        decoded = decode_temperature_payload(payload)

    elif device_type == "smoke_detector":
        decoded = decode_smoke_payload(payload)

    else:
        decoded = {"raw_payload": payload}

    insert_data(
        site,
        device_name,
        device_type,
        device_eui,
        "HTTP_API",
        None,
        None,
        str(decoded),
        None
    )

    return jsonify({
        "status": "success",
        "decoded": decoded
    })









@app.route("/smoke_data")
def smoke_data():
    site = request.args.get("site")
    device_eui = request.args.get("device_eui")

    rows = get_data_by_device(site, device_eui)
    rows = rows[::-1]

    data = {
        "timestamps": [],
        "fault_alarm": [],
        "smoke_alarm": [],
        "tamper_alarm": [],
        "voltage_alarm": [],
        "status": "OFFLINE",
        "last_seen_seconds": None
    }

    for r in rows:
        try:
            decoded = json.loads(r[8].replace("'", '"'))

            data["timestamps"].append(str(r[10]))
            data["fault_alarm"].append(decoded.get("fault_alarm"))
            data["smoke_alarm"].append(decoded.get("smoke_alarm"))
            data["tamper_alarm"].append(decoded.get("tamper_alarm"))
            data["voltage_alarm"].append(decoded.get("voltage_alarm"))

        except Exception as e:
            print("Smoke data error:", e)

    if len(data["timestamps"]) > 0:
        try:
            last_time = datetime.fromisoformat(data["timestamps"][-1])
            diff = (datetime.now() - last_time).total_seconds()

            data["last_seen_seconds"] = int(diff)
            data["status"] = "ONLINE" if diff < 60 else "OFFLINE"

        except Exception as e:
            print("Smoke status error:", e)

    if len(data["timestamps"]) > 0:
        anomaly = analyze_smoke(
            fault=data["fault_alarm"][-1],
            smoke=data["smoke_alarm"][-1],
            tamper=data["tamper_alarm"][-1],
            voltage=data["voltage_alarm"][-1],
            status=data["status"]
        )
    else:
        anomaly = analyze_smoke(status="OFFLINE")

    data.update(anomaly)

    return jsonify(data)





@app.route("/")
def dashboard():
    check = require_role("admin", "operator", "viewer")
    if check:
        return check
    rows = get_all_data()

    packets = []
    for r in rows:
        packets.append({
            "freq": r[1],
            "rssi": r[2],
            "snr": r[3],
            "payload": r[4],
            "temp": r[5]
        })

    temp = packets[0]["temp"] if packets else None

    return render_template(
        "index.html",
        packets=packets,
        temp=temp,
        role=session.get("role")
    )



@app.route("/data")
def data():
    site = request.args.get("site")
    device_eui = request.args.get("device_eui")

    rows = get_data_by_device(site, device_eui)
    rows = rows[::-1]
    data = {
        "timestamps": [],
        "voltage1": [], "voltage2": [], "voltage3": [],
        "current1": [], "current2": [], "current3": [],
        "power1": [], "power2": [], "power3": [],
        "pf1": [], "pf2": [], "pf3": [],
        "apparent1": [], "apparent2": [], "apparent3": [],
        "frequency": [],
        "total_power": [],
        "total_pf": [],
        "total_apparent": [],
        "energy": []
        
    }

    for r in rows:
        try:
            decoded = json.loads(r[8].replace("'", '"'))
            data["timestamps"].append(str(r[10]))

            v1 = decoded.get("voltage1")
            data["voltage1"].append(round(v1, 2) if v1 is not None and 0 <= v1 <= 300 else None)

            v2 = decoded.get("voltage2")
            data["voltage2"].append(round(v2, 2) if v2 is not None and 0 <= v2 <= 300 else None)

            v3 = decoded.get("voltage3")
            data["voltage3"].append(round(v3, 2) if v3 is not None and 0 <= v3 <= 300 else None)

            c1 = decoded.get("current1")
            data["current1"].append(round(c1, 2) if c1 is not None and 0 <= c1 <= 500 else None)

            c2 = decoded.get("current2")
            data["current2"].append(round(c2, 2) if c2 is not None and 0 <= c2 <= 500 else None)

            c3 = decoded.get("current3")
            data["current3"].append(round(c3, 2) if c3 is not None and 0 <= c3 <= 500 else None)

            p1 = decoded.get("power1")
            data["power1"].append(round(p1, 2) if p1 is not None and 0 <= p1 <= 10000 else None)

            p2 = decoded.get("power2")
            data["power2"].append(round(p2, 2) if p2 is not None and 0 <= p2 <= 10000 else None)

            p3 = decoded.get("power3")
            data["power3"].append(round(p3, 2) if p3 is not None and 0 <= p3 <= 10000 else None)

            pf1 = decoded.get("pf1")
            data["pf1"].append(round(pf1, 3) if pf1 is not None and 0 <= pf1 <= 1.2 else None)

            pf2 = decoded.get("pf2")
            data["pf2"].append(round(pf2, 3) if pf2 is not None and 0 <= pf2 <= 1.2 else None)

            pf3 = decoded.get("pf3")
            data["pf3"].append(round(pf3, 3) if pf3 is not None and 0 <= pf3 <= 1.2 else None)

            ap1 = decoded.get("apparent1")
            data["apparent1"].append(round(ap1, 2) if ap1 is not None and 0 <= ap1 <= 10000 else None)

            ap2 = decoded.get("apparent2")
            data["apparent2"].append(round(ap2, 2) if ap2 is not None and 0 <= ap2 <= 10000 else None)

            ap3 = decoded.get("apparent3")
            data["apparent3"].append(round(ap3, 2) if ap3 is not None and 0 <= ap3 <= 10000 else None)

            f = decoded.get("frequency")
            data["frequency"].append(round(f, 2) if f is not None and 40 <= f <= 70 else None)

            tp = decoded.get("total_power")
            data["total_power"].append(round(tp, 2) if tp is not None and 0 <= tp <= 10000 else None)

            tpf = decoded.get("total_pf")
            data["total_pf"].append(round(tpf, 3) if tpf is not None and 0 <= tpf <= 1.2 else None)

            tap = decoded.get("total_apparent")
            data["total_apparent"].append(round(tap, 2) if tap is not None and 0 <= tap <= 10000 else None)

            e = decoded.get("energy")
            data["energy"].append(round(e, 2) if e is not None and e >= 0 else None)

        except Exception as e:
            print("Data parse error:", e)
            continue

    # ---------------- DEVICE STATUS ----------------
    status = "OFFLINE"
    last_seen_seconds = None

    if len(data["timestamps"]) > 0:
        last_time_str = data["timestamps"][-1]

        try:
            last_time = datetime.fromisoformat(last_time_str)
            now = datetime.now()
            diff = (now - last_time).total_seconds()

            last_seen_seconds = int(diff)

            if diff < 60:
                status = "ONLINE"
            else:
                status = "OFFLINE"

        except Exception as e:
            print("Status parse error:", e)
            status = "UNKNOWN"
    
    # ---------------- ALARMS ----------------
    alerts = []

    if data["voltage1"] and data["voltage1"][-1] is not None:
        v = data["voltage1"][-1]
        if v > 250:
            alerts.append("⚠️ Overvoltage")
        elif v < 200:
            alerts.append("⚠️ Undervoltage")

    if data["current1"] and data["current1"][-1] is not None:
        c = data["current1"][-1]
        if c > 20:
            alerts.append("⚠️ Overcurrent")

    if data["pf1"] and data["pf1"][-1] is not None:
        pf = data["pf1"][-1]
        if pf < 0.7:
            alerts.append("⚠️ Low Power Factor")

    if data["frequency"] and data["frequency"][-1] is not None:
        f = data["frequency"][-1]
        if f < 49 or f > 51:
            alerts.append("⚠️ Frequency abnormal")

    print("Last 10 voltage1:", data["voltage1"][-10:])
    print("Last 10 current1:", data["current1"][-10:])
    print("Last 10 power1:", data["power1"][-10:])
    print("Last 10 pf1:", data["pf1"][-10:])
    print("Last 10 apparent1:", data["apparent1"][-10:])
    print("Last 10 frequency:", data["frequency"][-10:])

    if len(data["timestamps"]) > 0:
        anomaly = analyze_ac_meter(
            voltage1=data["voltage1"][-1],
            current1=data["current1"][-1],
            frequency=data["frequency"][-1],
            pf1=data["pf1"][-1],
            status=status
        )
    else:
        anomaly = analyze_ac_meter(status="OFFLINE")

    return jsonify({
        **data,
        "status": status,
        "alerts": alerts,
        "last_seen_seconds": last_seen_seconds,
        **anomaly
    })
            

@app.route("/packets")
def get_packets():
    site = request.args.get("site")
    device_eui = request.args.get("device_eui")

    rows = get_data_by_device(site, device_eui)

    data = []
    for r in rows:
        decoded = {}

        try:
            # Only decode if it's a string (JSON)
            if isinstance(r[4], str):
                decoded = json.loads(r[8].replace("'", '"'))
                
        except:
            decoded = {}

        # If decoded is not a dict, fix it
        if not isinstance(decoded, dict):
            decoded = {}

        data.append({
            "timestamp": str(r[10]),

            "voltage1": round(decoded.get("voltage1"), 2) if decoded.get("voltage1") is not None else None,
            "voltage2": round(decoded.get("voltage2"), 2) if decoded.get("voltage2") is not None else None,
            "voltage3": round(decoded.get("voltage3"), 2) if decoded.get("voltage3") is not None else None,

            "current1": round(decoded.get("current1"), 2) if decoded.get("current1") is not None else None,
            "current2": round(decoded.get("current2"), 2) if decoded.get("current2") is not None else None,
            "current3": round(decoded.get("current3"), 2) if decoded.get("current3") is not None else None,

            "power1": round(decoded.get("power1"), 2) if decoded.get("power1") is not None else None,
            "power2": round(decoded.get("power2"), 2) if decoded.get("power2") is not None else None,
            "power3": round(decoded.get("power3"), 2) if decoded.get("power3") is not None else None,
            "total_power": round(decoded.get("total_power"), 2) if decoded.get("total_power") is not None else None,

            "apparent1": round(decoded.get("apparent1"), 2) if decoded.get("apparent1") is not None else None,
            "apparent2": round(decoded.get("apparent2"), 2) if decoded.get("apparent2") is not None else None,
            "apparent3": round(decoded.get("apparent3"), 2) if decoded.get("apparent3") is not None else None,
            "total_apparent": round(decoded.get("total_apparent"), 2) if decoded.get("total_apparent") is not None else None,

            "pf1": round(decoded.get("pf1"), 3) if decoded.get("pf1") is not None else None,
            "pf2": round(decoded.get("pf2"), 3) if decoded.get("pf2") is not None else None,
            "pf3": round(decoded.get("pf3"), 3) if decoded.get("pf3") is not None else None,

            "total_pf": round(decoded.get("total_pf"), 3) if decoded.get("total_pf") is not None else None,
            "total_apparent": round(decoded.get("total_apparent"), 2) if decoded.get("total_apparent") is not None else None,

            "frequency": round(decoded.get("frequency"), 2) if decoded.get("frequency") is not None else None,
            "energy": round(decoded.get("energy"), 2) if decoded.get("energy") is not None else None,
        })
        

    return jsonify(data)

@app.route("/metrics")
def metrics():
    rows = get_all_data()

    data = {
        "voltage1": [],
        "voltage2": [],
        "voltage3": [],
        "current1": [],
        "current2": [],
        "current3": [],
        "frequency": [],
    }

    for r in rows:
        try:
            decoded = json.loads(r[4].replace("'", '"'))

            # ✅ FIX: ensure it's a dictionary
            if not isinstance(decoded, dict):
                continue

            data["voltage1"].append(decoded.get("voltage1", 0))
            data["voltage2"].append(decoded.get("voltage2", 0))
            data["voltage3"].append(decoded.get("voltage3", 0))

            data["current1"].append(decoded.get("current1", 0))
            data["current2"].append(decoded.get("current2", 0))
            data["current3"].append(decoded.get("current3", 0))

            data["frequency"].append(decoded.get("frequency", 0))

        except:
            continue
    
    return jsonify(data)



# ---------------- UTILITIES ----------------
def format_duration(seconds):
    seconds = int(seconds)

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"



# ---------------- ALERT HISTORY ----------------
from datetime import datetime

@app.route("/alert_history")
def alert_history():
    site = request.args.get("site")
    device_eui = request.args.get("device_eui")

    rows = get_data_by_device(site, device_eui)
    rows = rows[::-1]

    history = []

    previous_state = {
        "overcurrent": False,
        "overvoltage": False,
        "undervoltage": False,
        "low_pf": False,
        "freq": False
    }

    start_times = {
        "overcurrent": None,
        "overvoltage": None,
        "undervoltage": None,
        "low_pf": None,
        "freq": None
    }

    for r in rows:
        try:
            decoded = json.loads(r[8].replace("'", '"'))
            timestamp_str = str(r[10])
            timestamp = datetime.fromisoformat(timestamp_str)
           
            

            v1 = decoded.get("voltage1")
            c1 = decoded.get("current1")
            pf1 = decoded.get("pf1")
            f = decoded.get("frequency")

            # ---------- OVERCURRENT ----------
            current = c1 is not None and c1 > 20

            if current and not previous_state["overcurrent"]:
                start_times["overcurrent"] = timestamp
                history.append({
                    "timestamp": timestamp_str,
                    "message": "Overcurrent START",
                    "severity": "critical"
                })

            elif not current and previous_state["overcurrent"]:
                start = start_times["overcurrent"]
                duration = format_duration((timestamp - start).total_seconds()) if start else "0s"

                history.append({
                        "timestamp": timestamp_str,
                        "message": f"Overcurrent CLEARED ({duration})",
                        "severity": "info"
                    })

            previous_state["overcurrent"] = current

            # ---------- UNDERVOLTAGE ----------
            current = v1 is not None and v1 < 200

            if current and not previous_state["undervoltage"]:
                start_times["undervoltage"] = timestamp
                history.append({
                    "timestamp": timestamp_str,
                    "message": "Undervoltage START",
                    "severity": "warning"
                })

            elif not current and previous_state["undervoltage"]:
                start = start_times["undervoltage"]
                duration = format_duration((timestamp - start).total_seconds()) if start else "0s"

                history.append({
                    "timestamp": timestamp_str,
                    "message": f"Undervoltage CLEARED ({duration})",
                    "severity": "info"
                })

            previous_state["undervoltage"] = current

            # ---------- FREQUENCY ----------
            current = f is not None and (f < 49 or f > 51)

            if current and not previous_state["freq"]:
                start_times["freq"] = timestamp
                history.append({
                    "timestamp": timestamp_str,
                    "message": "Frequency abnormal START",
                    "severity": "warning"
                })

            elif not current and previous_state["freq"]:
                start = start_times["freq"]
                duration = format_duration((timestamp - start).total_seconds()) if start else "0s"

                history.append({
                    "timestamp": timestamp_str,
                    "message": f"Frequency abnormal CLEARED ({duration})",
                    "severity": "info"
                })
            previous_state["freq"] = current

            # ---------- LOW PF ----------
            current = pf1 is not None and pf1 < 0.7

            if current and not previous_state["low_pf"]:
                start_times["low_pf"] = timestamp
                history.append({
                    "timestamp": timestamp_str,
                    "message": "Low Power Factor START",
                    "severity": "warning"
                })

            elif not current and previous_state["low_pf"]:
                start = start_times["low_pf"]
                duration = format_duration((timestamp - start).total_seconds()) if start else "0s"

                history.append({
                    "timestamp": timestamp_str,
                    "message": f"Low Power Factor CLEARED ({duration})",
                    "severity": "info"
                })

            previous_state["low_pf"] = current

        except Exception as e:
            print("Alert history error:", e)
            continue

    return jsonify(history[-20:])


@app.route("/devices")
def devices():
    rows = get_devices()

    data = []
    for r in rows:
        data.append({
            "site": r[0],
            "device_name": r[1],
            "device_type": r[2],
            "device_eui": r[3]
        })

    return jsonify(data)


@app.route("/temperature_data")
def temperature_data():
    site = request.args.get("site")
    device_eui = request.args.get("device_eui")

    rows = get_data_by_device(site, device_eui)
    rows = rows[::-1]

    data = {
        "timestamps": [],
        "event": [],
        "state": [],
        "battery": [],
        "movement": [],
        "temperature": [],
        "humidity": [],
        "status": "OFFLINE",
        "last_seen_seconds": None
    }

    for r in rows:
        try:
            decoded = json.loads(r[8].replace("'", '"'))

            data["timestamps"].append(str(r[10]))
            data["event"].append(decoded.get("event"))
            data["state"].append(decoded.get("state"))
            data["battery"].append(decoded.get("battery"))
            data["movement"].append(decoded.get("movement"))
            data["temperature"].append(decoded.get("temperature"))
            data["humidity"].append(decoded.get("humidity"))

        except Exception as e:
            print("Temperature data error:", e)

    if len(data["timestamps"]) > 0:
        try:
            last_time = datetime.fromisoformat(data["timestamps"][-1])
            diff = (datetime.now() - last_time).total_seconds()

            data["last_seen_seconds"] = int(diff)
            data["status"] = "ONLINE" if diff < 60 else "OFFLINE"

        except Exception as e:
            print("Temperature status error:", e)

    if len(data["timestamps"]) > 0:
        anomaly = analyze_temperature(
            temperature=data["temperature"][-1],
            humidity=data["humidity"][-1],
            battery=data["battery"][-1],
            status=data["status"]
        )
    else:
        anomaly = analyze_temperature(status="OFFLINE")

    data.update(anomaly)

    return jsonify(data)




@app.route("/device_status_summary")
def device_status_summary():
    devices = get_devices()

    total_devices = len(devices)
    total_sites = len(set(d[0] for d in devices))

    online = 0
    offline = 0

    for d in devices:
        site = d[0]
        device_eui = d[3]   # keep this because your print showed correct EUI

        rows = get_data_by_device(site, device_eui)

        if rows:
            try:
                latest_row = rows[0]   # newest row
                last_time = datetime.strptime(str(latest_row[10]), "%Y-%m-%d %H:%M:%S")
                diff = (datetime.now() - last_time).total_seconds()

                if diff < 60:
                    online += 1
                else:
                    offline += 1

            except Exception as e:
                print("Summary time error:", e)
                offline += 1
        else:
            offline += 1

    return jsonify({
        "sites": total_sites,
        "devices": total_devices,
        "online": online,
        "offline": offline
    })



# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
