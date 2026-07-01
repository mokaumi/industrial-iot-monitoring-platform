from werkzeug.security import generate_password_hash, check_password_hash
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, session
import paho.mqtt.client as mqtt
import threading
import socket
import sqlite3
import json
from datetime import datetime
from database import (
    init_db, insert_data, get_all_data, get_data_by_device, get_devices,
    init_users_table, get_user_by_username, create_user, get_all_users,
    delete_user_by_id, insert_anomaly_event, recent_anomaly_exists, 
    get_recent_anomaly_events, get_device_anomaly_stats, get_assets_by_site, get_data_by_asset
)
from auth import register_auth_routes, require_role
from admin import register_admin_routes
from decoders import decode_payload, decode_temperature_payload, decode_smoke_payload
from mqtt_handler import mqtt_listener
from udp_handler import udp_listener
import base64
import os
from anomaly import analyze_temperature, analyze_smoke, analyze_ac_meter, predict_temperature_trend
from postgres_db import get_pg_connection
from postgres_db import get_data_by_asset_pg   
from postgres_db import insert_anomaly_event_pg 
from postgres_db import recent_anomaly_exists_pg
from postgres_db import resolve_open_incidents_pg
from postgres_db import acknowledge_active_alarm_pg
import paho.mqtt.publish as publish
from postgres_db import log_gateway_event_pg


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
threading.Thread(target=mqtt_listener, daemon=True).start()



@app.route("/retry_iot_device_command/<int:command_id>", methods=["POST"])
def retry_iot_device_command(command_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT device_id, command_name
        FROM iot_device_commands
        WHERE id=%s
    """, (command_id,))

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return jsonify({
            "success": False,
            "message": "Device command not found"
        }), 404

    cur.execute("""
        INSERT INTO iot_device_commands
        (device_id, command_name, command_status, issued_by)
        VALUES (%s, %s, 'PENDING', 'Admin')
    """, (row[0], row[1]))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "message": "Device command retry queued"
    })





@app.route("/iot_device_command_queue/<int:device_id>")
def iot_device_command_queue(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            command_name,
            command_status,
            issued_by,
            issued_at,
            completed_at
        FROM iot_device_commands
        WHERE device_id=%s
        ORDER BY id DESC
        LIMIT 20
    """, (device_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "command_name": r[1],
            "command_status": r[2],
            "issued_by": r[3],
            "issued_at": str(r[4]),
            "completed_at": str(r[5]) if r[5] else "-"
        }
        for r in rows
    ])


@app.route("/issue_iot_device_command/<int:device_id>", methods=["POST"])
def issue_iot_device_command(device_id):
    data = request.get_json()

    command = data.get("command")

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO iot_device_commands
        (
            device_id,
            command_name,
            command_status,
            issued_by
        )
        VALUES (%s, %s, 'PENDING', 'Admin')
    """, (
        device_id,
        command
    ))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "message": "Device command queued successfully"
    })




@app.route("/update_iot_device_config/<int:device_id>", methods=["POST"])
def update_iot_device_config(device_id):
    data = request.get_json()

    device_name = data.get("device_name")
    device_type = data.get("device_type")
    site = data.get("site")
    firmware_version = data.get("firmware_version")
    is_active = data.get("is_active")

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE iot_devices
        SET
            device_name=%s,
            device_type=%s,
            site=%s,
            firmware_version=%s,
            is_active=%s
        WHERE id=%s
    """, (
        device_name,
        device_type,
        site,
        firmware_version,
        is_active,
        device_id
    ))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "message": "Device configuration updated successfully"
    })




@app.route("/iot_device_config_page/<int:device_id>")
def iot_device_config_page(device_id):
    return render_template(
        "iot_device_config.html",
        device_id=device_id
    )





@app.route("/iot_device_alarm_history/<int:device_id>")
def iot_device_alarm_history(device_id):

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            alarm_name,
            severity,
            status,
            description,
            first_seen,
            last_seen
        FROM iot_device_alarms
        WHERE device_id=%s
        ORDER BY last_seen DESC
    """,(device_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id":r[0],
            "alarm_name":r[1],
            "severity":r[2],
            "status":r[3],
            "description":r[4],
            "first_seen":str(r[5]),
            "last_seen":str(r[6])
        }
        for r in rows
    ])




@app.route("/ack_iot_device_alarm/<int:alarm_id>", methods=["POST"])
def ack_iot_device_alarm(alarm_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE iot_device_alarms
        SET status='ACKNOWLEDGED'
        WHERE id=%s
          AND status='ACTIVE'
    """, (alarm_id,))

    updated = cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    if updated == 0:
        return jsonify({
            "success": False,
            "message": "Alarm cannot be acknowledged"
        })

    return jsonify({
        "success": True,
        "message": "Device alarm acknowledged"
    })





@app.route("/iot_device_alarm_summary/<int:device_id>")
def iot_device_alarm_summary(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE severity='CRITICAL' AND status='ACTIVE'),
            COUNT(*) FILTER (WHERE severity='WARNING' AND status='ACTIVE'),
            COUNT(*) FILTER (WHERE status='ACTIVE')
        FROM iot_device_alarms
        WHERE device_id=%s
    """, (device_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({
        "total": row[0],
        "critical": row[1],
        "warning": row[2],
        "active": row[3]
    })




@app.route("/iot_device_alarms/<int:device_id>")
def iot_device_alarms(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            alarm_name,
            severity,
            status,
            description,
            first_seen,
            last_seen
        FROM iot_device_alarms
        WHERE device_id=%s
          AND status='ACTIVE'
        ORDER BY last_seen DESC
    """, (device_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "alarm_name": r[1],
            "severity": r[2],
            "status": r[3],
            "description": r[4],
            "first_seen": str(r[5]),
            "last_seen": str(r[6])
        }
        for r in rows
    ])


@app.route("/iot_device_telemetry/<int:device_id>")
def get_iot_device_telemetry(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            telemetry_key,
            telemetry_value,
            unit,
            recorded_at
        FROM iot_device_telemetry
        WHERE device_id=%s
        ORDER BY recorded_at DESC
    """, (device_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "telemetry_key": r[0],
            "telemetry_value": r[1],
            "unit": r[2],
            "recorded_at": str(r[3])
        }
        for r in rows
    ])





@app.route("/iot_device_details/<int:device_id>")
def iot_device_details(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            d.id,
            d.device_eui,
            d.device_name,
            d.device_type,
            d.site,
            d.firmware_version,
            d.battery_level,
            d.rssi,
            d.snr,
            d.status,
            d.is_active,
            d.last_seen,
            g.gateway_name
        FROM iot_devices d
        JOIN gateways g
            ON d.gateway_id = g.id
        WHERE d.id=%s
    """, (device_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Device not found"}), 404

    return jsonify({
        "id": row[0],
        "device_eui": row[1],
        "device_name": row[2],
        "device_type": row[3],
        "site": row[4],
        "firmware_version": row[5],
        "battery_level": row[6],
        "rssi": row[7],
        "snr": row[8],
        "status": row[9],
        "is_active": row[10],
        "last_seen": str(row[11]) if row[11] else "-",
        "gateway_name": row[12]
    })





@app.route("/iot_device_details_page/<int:device_id>")
def iot_device_details_page(device_id):
    return render_template(
        "iot_device_details.html",
        device_id=device_id
    )




@app.route("/gateway_device_summary/<int:gateway_id>")
def gateway_device_summary(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE status='ONLINE'),
            COUNT(*) FILTER (WHERE status='OFFLINE'),
            ROUND(AVG(battery_level), 1)
        FROM iot_devices
        WHERE gateway_id=%s
    """, (gateway_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({
        "total": row[0],
        "online": row[1],
        "offline": row[2],
        "avg_battery": float(row[3]) if row[3] else 0
    })




@app.route("/gateway_iot_devices/<int:gateway_id>")
def gateway_iot_devices(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            device_eui,
            device_name,
            device_type,
            site,
            firmware_version,
            battery_level,
            rssi,
            snr,
            status,
            is_active,
            last_seen
        FROM iot_devices
        WHERE gateway_id=%s
        ORDER BY id ASC
    """, (gateway_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "device_eui": r[1],
            "device_name": r[2],
            "device_type": r[3],
            "site": r[4],
            "firmware_version": r[5],
            "battery_level": r[6],
            "rssi": r[7],
            "snr": r[8],
            "status": r[9],
            "is_active": r[10],
            "last_seen": str(r[11]) if r[11] else "-"
        }
        for r in rows
    ])

    
@app.route("/cancel_gateway_command/<int:command_id>", methods=["POST"])
def cancel_gateway_command(command_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE gateway_commands
        SET command_status='CANCELLED',
            completed_at=CURRENT_TIMESTAMP
        WHERE id=%s
          AND command_status='PENDING'
    """, (command_id,))

    updated = cur.rowcount

    cur.execute("""
        SELECT g.gateway_eui, c.command_name
        FROM gateway_commands c
        JOIN gateways g ON c.gateway_id = g.id
        WHERE c.id = %s
    """, (command_id,))

    row = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    if updated == 0:
        return jsonify({
            "success": False,
            "message": "Command cannot be cancelled"
        })

    if row:
        log_gateway_event_pg(
            row[0],
            "COMMAND_CANCELLED",
            f"{row[1]} cancelled"
        )

    return jsonify({
        "success": True,
        "message": "Command cancelled successfully"
    })



@app.route("/retry_gateway_command/<int:command_id>", methods=["POST"])
def retry_gateway_command(command_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT gateway_id, command_name
        FROM gateway_commands
        WHERE id=%s
    """, (command_id,))

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return jsonify({
            "success": False,
            "message": "Command not found"
        }), 404

    gateway_id = row[0]
    command_name = row[1]

    cur.execute("""
        INSERT INTO gateway_commands
        (
            gateway_id,
            command_name,
            command_status,
            issued_by
        )
        VALUES (%s,%s,'PENDING','Admin')
    """, (
        gateway_id,
        command_name
    ))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "message": "Command retry queued successfully"
    })




@app.route("/gateway_command_summary/<int:gateway_id>")
def gateway_command_summary(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE command_status='SUCCESS'),
            COUNT(*) FILTER (WHERE command_status='FAILED'),
            COUNT(*) FILTER (WHERE command_status IN ('PENDING','RUNNING'))
        FROM gateway_commands
        WHERE gateway_id=%s
    """, (gateway_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({
        "total": row[0],
        "success": row[1],
        "failed": row[2],
        "pending": row[3]
    })




@app.route("/gateway_command_queue/<int:gateway_id>")
def gateway_command_queue(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            command_name,
            command_status,
            issued_by,
            issued_at,
            completed_at
        FROM gateway_commands
        WHERE gateway_id = %s
        ORDER BY id DESC
        LIMIT 20
    """, (gateway_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "command_name": r[1],
            "command_status": r[2],
            "issued_by": r[3],
            "issued_at": str(r[4]),
            "completed_at": str(r[5]) if r[5] else "-"
        }
        for r in rows
    ])





@app.route("/issue_gateway_command/<int:gateway_id>", methods=["POST"])
def issue_gateway_command(gateway_id):

    data = request.get_json()

    command = data.get("command")

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO gateway_commands
        (
            gateway_id,
            command_name,
            command_status,
            issued_by
        )
        VALUES
        (%s,%s,'PENDING','Admin')
    """,
    (
        gateway_id,
        command
    ))

    conn.commit()

    cur.close()
    conn.close()

    return jsonify({
        "success":True,
        "message":"Command queued successfully"
    })





@app.route("/update_gateway_config/<int:gateway_id>", methods=["POST"])
def update_gateway_config(gateway_id):
    data = request.get_json()

    gateway_name = data.get("gateway_name")
    latitude = data.get("latitude")
    longitude = data.get("longitude")

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE gateways
        SET
            gateway_name = %s,
            latitude = %s,
            longitude = %s
        WHERE id = %s
    """, (
        gateway_name,
        latitude,
        longitude,
        gateway_id
    ))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "message": "Gateway configuration updated successfully"
    })






@app.route("/gateway_config_page/<int:gateway_id>")
def gateway_config_page(gateway_id):
    return render_template(
        "gateway_config.html",
        gateway_id=gateway_id
    )


@app.route("/gateway_config/<int:gateway_id>")
def gateway_config(gateway_id):

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            gateway_name,
            gateway_eui,
            latitude,
            longitude,
            status
        FROM gateways
        WHERE id=%s
    """,(gateway_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return jsonify({"error":"Gateway not found"}),404

    return jsonify({

        "id":row[0],
        "gateway_name":row[1],
        "gateway_eui":row[2],
        "latitude":row[3],
        "longitude":row[4],
        "status":row[5]

    })




@app.route("/fleet_map_gateways")
def fleet_map_gateways():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            gateway_eui,
            gateway_name,
            status,
            latitude,
            longitude,
            last_seen
        FROM gateways
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY id ASC
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "gateway_eui": r[1],
            "gateway_name": r[2],
            "status": r[3],
            "latitude": r[4],
            "longitude": r[5],
            "last_seen": str(r[6]) if r[6] else "-"
        }
        for r in rows
    ])


@app.route("/fleet_events")
def fleet_events():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            e.id,
            e.gateway_eui,
            g.gateway_name,
            e.event_type,
            e.event_message,
            e.created_at
        FROM gateway_events e
        LEFT JOIN gateways g
            ON e.gateway_eui = g.gateway_eui
        ORDER BY e.id DESC
        LIMIT 20
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "gateway_eui": r[1],
            "gateway_name": r[2],
            "event_type": r[3],
            "event_message": r[4],
            "created_at": str(r[5])
        }
        for r in rows
    ])




@app.route("/fleet_active_alarms")
def fleet_active_alarms():

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            a.id,
            g.id,
            g.gateway_name,
            a.gateway_eui,
            a.parameter,
            a.severity,
            a.alarm_status,
            a.last_seen
        FROM gateway_alarms a
        JOIN gateways g
            ON a.gateway_eui = g.gateway_eui
        WHERE a.alarm_status IN ('OPEN','ACKNOWLEDGED')
        ORDER BY a.last_seen DESC
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "alarm_id": r[0],
            "gateway_id": r[1],
            "gateway_name": r[2],
            "gateway_eui": r[3],
            "parameter": r[4],
            "severity": r[5],
            "alarm_status": r[6],
            "last_seen": str(r[7])
        }
        for r in rows
    ])




@app.route("/fleet_alarm_summary")
def fleet_alarm_summary():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE severity = 'CRITICAL'),
            COUNT(*) FILTER (WHERE severity = 'WARNING'),
            COUNT(DISTINCT gateway_eui)
        FROM gateway_alarms
        WHERE alarm_status IN ('OPEN', 'ACKNOWLEDGED')
    """)

    row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({
        "critical": row[0],
        "warning": row[1],
        "gateways_in_alarm": row[2]
    })





@app.route("/fleet_health")
def fleet_health():

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT status
    FROM gateways
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    if not rows:
        return jsonify({"health": 0})

    total_score = 0

    for r in rows:

        status = r[0]

        if status == "ONLINE":
            total_score += 100

        elif status == "DEGRADED":
            total_score += 60

        else:
            total_score += 0

    fleet_health = round(
        total_score / len(rows),
        1
    )

    return jsonify({
        "health": fleet_health
    })




@app.route("/fleet_gateways")
def fleet_gateways():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            gateway_eui,
            gateway_name,
            status,
            last_seen
        FROM gateways
        ORDER BY id ASC
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "gateway_eui": r[1],
            "gateway_name": r[2],
            "status": r[3],
            "last_seen": str(r[4]) if r[4] else "-"
        }
        for r in rows
    ])



@app.route("/fleet_dashboard")
def fleet_dashboard():
    return render_template("fleet_dashboard.html")



@app.route("/fleet_summary")
def fleet_summary():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM gateways
    """)
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM gateways
        WHERE status = 'ONLINE'
    """)
    online = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM gateways
        WHERE status = 'DEGRADED'
    """)
    degraded = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM gateways
        WHERE status = 'OFFLINE'
    """)
    offline = cur.fetchone()[0]
    
    available = online + degraded

    if total == 0:
        availability = 0
    else:
        availability = round((available / total) * 100, 2)

    cur.close()
    conn.close()

    return jsonify({
        "total": total,
        "online": online,
        "degraded": degraded,
        "offline": offline,
        "availability": availability
    })





@app.route("/gateway_events/<int:gateway_id>")
def gateway_events(gateway_id):

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT gateway_eui
    FROM gateways
    WHERE id=%s
    """, (gateway_id,))

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return jsonify([])

    gateway_eui = row[0]

    cur.execute("""
    SELECT
        id,
        event_type,
        event_message,
        created_at
    FROM gateway_events
    WHERE gateway_eui=%s
    ORDER BY id DESC
    LIMIT 100
    """, (gateway_eui,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "event_type": r[1],
            "event_message": r[2],
            "created_at": str(r[3])
        }
        for r in rows
    ])










@app.route("/gateway_telemetry_history/<int:gateway_id>")
def gateway_telemetry_history(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT gateway_eui
        FROM gateways
        WHERE id = %s
    """, (gateway_id,))

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return jsonify([])

    gateway_eui = row[0]

    cur.execute("""
        SELECT
            timestamp,
            cpu_usage,
            memory_usage,
            signal_quality,
            packets_today,
            status
        FROM gateway_telemetry
        WHERE gateway_eui = %s
        ORDER BY timestamp DESC
        LIMIT 50
    """, (gateway_eui,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    rows = list(reversed(rows))

    return jsonify([
        {
            "timestamp": str(r[0]),
            "cpu_usage": r[1],
            "memory_usage": r[2],
            "signal_quality": r[3],
            "packets_today": r[4],
            "status": r[5]
        }
        for r in rows
    ])




@app.route("/gateway_uptime/<int:gateway_id>")
def gateway_uptime(gateway_id):

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT online_since
    FROM gateways
    WHERE id=%s
    """, (gateway_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row or not row[0]:
        return jsonify({"uptime":"--"})

    delta = datetime.now() - row[0]

    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60

    return jsonify({
        "uptime":
        f"{days}d {hours}h {minutes}m"
    })






@app.route("/gateway_availability/<int:gateway_id>")
def gateway_availability(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT gateway_eui
        FROM gateways
        WHERE id = %s
    """, (gateway_id,))

    gateway = cur.fetchone()

    if not gateway:
        cur.close()
        conn.close()
        return jsonify({"availability": 0})

    gateway_eui = gateway[0]

    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (
                WHERE status IN ('ONLINE','DEGRADED')
            )
        FROM gateway_telemetry
        WHERE gateway_eui = %s
          AND timestamp::date = CURRENT_DATE
    """, (gateway_eui,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    total = row[0]
    available = row[1]

    if total == 0:
        availability = 0
    else:
        availability = round((available / total) * 100, 2)

    return jsonify({
        "availability": availability,
        "total_records": total,
        "available_records": available
    })




@app.route("/gateway_alarm_history_page")
def gateway_alarm_history_page():
    if "user" not in session:
        return redirect("/login")

    return render_template("gateway_alarm_history.html")




@app.route("/gateway_alarm_history")
def gateway_alarm_history():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            ga.id,
            ga.gateway_eui,
            g.gateway_name,
            ga.parameter,
            ga.severity,
            ga.alarm_status,
            ga.alarm_reason,
            ga.first_seen,
            ga.last_seen,
            ga.cleared_at
        FROM gateway_alarms ga
        LEFT JOIN gateways g
            ON ga.gateway_eui = g.gateway_eui
        ORDER BY ga.id DESC
        LIMIT 100
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "gateway_eui": r[1],
            "gateway_name": r[2],
            "parameter": r[3],
            "severity": r[4],
            "alarm_status": r[5],
            "alarm_reason": r[6],
            "first_seen": str(r[7]),
            "last_seen": str(r[8]),
            "cleared_at": str(r[9]) if r[9] else ""
        }
        for r in rows
    ])


@app.route("/ack_gateway_alarm/<int:alarm_id>", methods=["POST"])
def ack_gateway_alarm(alarm_id):
    if "user" not in session:
        return jsonify({"success": False, "message": "Not logged in"}), 401

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE gateway_alarms
        SET alarm_status = 'ACKNOWLEDGED'
        WHERE id = %s
          AND alarm_status = 'OPEN'
    """, (alarm_id,))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "message": "Gateway alarm acknowledged"
    })



@app.route("/gateway_alarm_summary/<int:gateway_id>")
def gateway_alarm_summary(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT gateway_eui
        FROM gateways
        WHERE id = %s
    """, (gateway_id,))

    gateway = cur.fetchone()

    if not gateway:
        cur.close()
        conn.close()
        return jsonify({"total": 0, "critical": 0, "warning": 0})

    gateway_eui = gateway[0]

    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE severity = 'CRITICAL'),
            COUNT(*) FILTER (WHERE severity = 'WARNING')
        FROM gateway_alarms
        WHERE gateway_eui = %s
          AND alarm_status = 'OPEN'
    """, (gateway_eui,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({
        "total": row[0],
        "critical": row[1],
        "warning": row[2]
    })







@app.route("/gateway_active_alarms/<int:gateway_id>")
def gateway_active_alarms(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT gateway_eui
        FROM gateways
        WHERE id = %s
    """, (gateway_id,))

    gateway = cur.fetchone()

    if not gateway:
        cur.close()
        conn.close()
        return jsonify([])

    gateway_eui = gateway[0]

    cur.execute("""
        SELECT
            id,
            parameter,
            severity,
            alarm_status,
            alarm_reason,
            first_seen,
            last_seen
        FROM gateway_alarms
        WHERE gateway_eui = %s
          AND alarm_status IN ('OPEN','ACKNOWLEDGED')
        ORDER BY id DESC
    """, (gateway_eui,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "parameter": r[1],
            "severity": r[2],
            "alarm_status": r[3],
            "alarm_reason": r[4],
            "first_seen": str(r[5]),
            "last_seen": str(r[6])
        }
        for r in rows
    ])




@app.route("/gateway_command_history/<int:gateway_id>")
def gateway_command_history(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT gateway_eui
        FROM gateways
        WHERE id = %s
    """, (gateway_id,))

    gateway = cur.fetchone()

    if not gateway:
        cur.close()
        conn.close()
        return jsonify([])

    gateway_eui = gateway[0]

    cur.execute("""
        SELECT
            command,
            status,
            message,
            source,
            response_time
        FROM gateway_command_responses
        WHERE gateway_eui = %s
        ORDER BY id DESC
        LIMIT 10
    """, (gateway_eui,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "command": r[0],
            "status": r[1],
            "message": r[2],
            "source": r[3],
            "response_time": str(r[4])
        }
        for r in rows
    ])



@app.route("/send_gateway_command/<int:gateway_id>", methods=["POST"])
def send_gateway_command(gateway_id):
    data = request.get_json()
    command = data.get("command")

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT gateway_eui, gateway_name
        FROM gateways
        WHERE id = %s
    """, (gateway_id,))

    gateway = cur.fetchone()

    cur.close()
    conn.close()

    if not gateway:
        return jsonify({
            "success": False,
            "message": "Gateway not found"
        }), 404

    gateway_eui = gateway[0]
    gateway_name = gateway[1]

    payload = {
        "gateway_eui": gateway_eui,
        "gateway_name": gateway_name,
        "command": command,
        "source": "dashboard"
    }

    topic = f"gateways/{gateway_eui}/commands"

    publish.single(
        topic,
        payload=json.dumps(payload),
        hostname="mosquitto",
        port=1883
    )
    
    log_gateway_event_pg(
        gateway_eui,
        "COMMAND_SENT",
        f"{command} command sent"
    )

    print(f"GATEWAY COMMAND PUBLISHED: {topic} -> {payload}")

    return jsonify({
        "success": True,
        "message": f"{command} command sent to {gateway_name}",
        "topic": topic
    })




@app.route("/gateway_health/<int:gateway_id>")
def gateway_health(gateway_id):

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT gateway_eui
    FROM gateways
    WHERE id = %s
    """, (gateway_id,))

    row = cur.fetchone()

    if not row:
        return jsonify({"error": "Gateway not found"}), 404

    gateway_eui = row[0]

    cur.execute("""
    SELECT
        cpu_usage,
        memory_usage,
        signal_quality,
        packets_today,
        status,
        timestamp
    FROM gateway_telemetry
    WHERE gateway_eui = %s
    ORDER BY timestamp DESC
    LIMIT 1
    """, (gateway_eui,))

    data = cur.fetchone()

    cur.close()
    conn.close()

    if not data:
        return jsonify({
            "status": "UNKNOWN"
        })

    return jsonify({
        "cpu_usage": data[0],
        "memory_usage": data[1],
        "signal_quality": data[2],
        "packets_today": data[3],
        "status": data[4],
        "last_seen": str(data[5])
    })






@app.route("/gateway_devices/<int:gateway_id>")
def gateway_devices(gateway_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT
        id,
        device_eui,
        device_name,
        device_type,
        site,
        is_active
    FROM devices
    WHERE gateway_id = %s
    ORDER BY id
    """, (gateway_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "device_eui": r[1],
            "device_name": r[2],
            "device_type": r[3],
            "site": r[4],
            "is_active": r[5]
        }
        for r in rows
    ])






@app.route("/gateway_details_page/<int:gateway_id>")
def gateway_details_page(gateway_id):
    if "user" not in session:
        return redirect("/login")

    return render_template("gateway_details.html", gateway_id=gateway_id)




@app.route("/gateway_details/<int:gateway_id>")
def gateway_details(gateway_id):

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT
        g.id,
        g.gateway_name,
        g.gateway_eui,
        g.status,
        COUNT(d.id)
    FROM gateways g
    LEFT JOIN devices d
        ON d.gateway_id = g.id
    WHERE g.id = %s
    GROUP BY g.id
    """, (gateway_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return jsonify({"error":"Gateway not found"}), 404

    return jsonify({
        "id": row[0],
        "gateway_name": row[1],
        "gateway_eui": row[2],
        "status": row[3],
        "connected_devices": row[4]
    })





@app.route("/gateway_registry_page")
def gateway_registry_page():
    if "user" not in session:
        return redirect("/login")

    return render_template("gateway_registry.html")






@app.route("/gateways_registry", methods=["POST"])
def add_gateway_registry():
    data = request.get_json()

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO gateways (
        gateway_eui,
        gateway_name,
        gateway_type,
        site,
        ip_address
    )
    VALUES (%s,%s,%s,%s,%s)
    RETURNING id
    """, (
        data.get("gateway_eui"),
        data.get("gateway_name"),
        data.get("gateway_type"),
        data.get("site"),
        data.get("ip_address")
    ))

    gateway_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "message": "Gateway added successfully",
        "gateway_id": gateway_id
    })





@app.route("/gateways_registry", methods=["GET"])
def get_gateways_registry():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT id, gateway_eui, gateway_name, gateway_type, site, ip_address, status, last_seen, is_active
    FROM gateways
    ORDER BY id DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "gateway_eui": r[1],
            "gateway_name": r[2],
            "gateway_type": r[3],
            "site": r[4],
            "ip_address": r[5],
            "status": r[6],
            "last_seen": str(r[7]) if r[7] else "",
            "is_active": r[8]
        }
        for r in rows
    ])






@app.route("/device_twin/<int:device_id>", methods=["POST"])
def update_device_twin(device_id):
    data = request.get_json()

    reporting_interval = data.get("reporting_interval")
    alarm_enabled = data.get("alarm_enabled")

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT device_eui, device_name
        FROM devices
        WHERE id = %s
    """, (device_id,))

    device = cur.fetchone()

    if not device:
        cur.close()
        conn.close()
        return jsonify({"success": False, "message": "Device not found"}), 404

    device_eui = device[0]
    device_name = device[1]

    cur.execute("""
        UPDATE device_twin
        SET
            desired_reporting_interval = %s,
            desired_alarm_enabled = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE device_eui = %s
    """, (
        reporting_interval,
        alarm_enabled,
        device_eui
    ))


    command_payload = {
        "device_eui": device_eui,
        "command": "update_config",
        "reporting_interval": reporting_interval,
        "alarm_enabled": alarm_enabled,
        "source": "device_twin"
    }

    command_topic = f"devices/{device_eui}/commands"

    publish.single(
        command_topic,
        payload=json.dumps(command_payload),
        hostname="mosquitto",
        port=1883
    )

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "message": f"Desired twin updated for {device_name}",
        "device_eui": device_eui
    })




@app.route("/device_twin/<int:device_id>")
def get_device_twin(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT d.device_eui, d.device_name
        FROM devices d
        WHERE d.id = %s
    """, (device_id,))

    device = cur.fetchone()

    if not device:
        cur.close()
        conn.close()
        return jsonify({"error": "Device not found"}), 404

    device_eui = device[0]
    device_name = device[1]

    cur.execute("""
        SELECT
            desired_reporting_interval,
            desired_alarm_enabled,
            reported_reporting_interval,
            reported_alarm_enabled,
            updated_at
        FROM device_twin
        WHERE device_eui = %s
    """, (device_eui,))

    twin = cur.fetchone()

    cur.close()
    conn.close()

    if not twin:
        return jsonify({"error": "Twin not found"}), 404

    return jsonify({
        "device_eui": device_eui,
        "device_name": device_name,
        "desired": {
            "reporting_interval": twin[0],
            "alarm_enabled": twin[1]
        },
        "reported": {
            "reporting_interval": twin[2],
            "alarm_enabled": twin[3]
        },
        "updated_at": str(twin[4])
    })



@app.route("/device_command_history/<int:device_id>")
def device_command_history(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT device_eui
        FROM devices
        WHERE id = %s
    """, (device_id,))

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return jsonify([])

    device_eui = row[0]

    cur.execute("""
        SELECT
            command,
            status,
            message,
            source,
            response_time
        FROM command_responses
        WHERE device_eui = %s
        ORDER BY id DESC
        LIMIT 10
    """, (device_eui,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify([
        {
            "command": r[0],
            "status": r[1],
            "message": r[2],
            "source": r[3],
            "response_time": str(r[4])
        }
        for r in rows
    ])


@app.route("/send_command/<int:device_id>", methods=["POST"])
def send_command(device_id):
    data = request.get_json()

    command = data.get("command")

    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT device_eui, device_name
        FROM devices
        WHERE id = %s
    """, (device_id,))

    device = cur.fetchone()

    cur.close()
    conn.close()

    if not device:
        return jsonify({
            "success": False,
            "message": "Device not found"
        }), 404

    device_eui = device[0]
    device_name = device[1]

    command_payload = {
        "device_eui": device_eui,
        "device_name": device_name,
        "command": command,
        "source": "dashboard"
    }

    command_topic = f"devices/{device_eui}/commands"

    publish.single(
        command_topic,
        payload=json.dumps(command_payload),
        hostname="mosquitto",
        port=1883
    )

    print(f"MQTT COMMAND PUBLISHED: {command_topic} -> {command_payload}")

    return jsonify({
        "success": True,
        "message": f"{command} command published to {device_name}",
        "device_eui": device_eui,
        "topic": command_topic
    })




@app.route("/device_activity/<int:device_id>")
def device_activity(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT device_eui
        FROM devices
        WHERE id = %s
    """, (device_id,))

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return jsonify({
            "last_packet": None,
            "packets_today": 0,
            "reporting_interval": "Unknown",
            "data_quality": "0%"
        })

    device_eui = row[0]

    cur.execute("""
        SELECT timestamp
        FROM sensor_data
        WHERE device_eui = %s
        ORDER BY timestamp DESC
        LIMIT 100
    """, (device_eui,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    if not rows:
        return jsonify({
            "last_packet": None,
            "packets_today": 0,
            "reporting_interval": "Unknown",
            "data_quality": "0%"
        })

    last_packet = rows[0][0]

    packets_today = sum(
        1 for r in rows
        if r[0].date() == last_packet.date()
    )

    if len(rows) >= 2:
        interval = abs((rows[0][0] - rows[1][0]).total_seconds())
        reporting_interval = f"{int(interval)} sec"
    else:
        reporting_interval = "Unknown"

    data_quality = "100%" if reporting_interval != "Unknown" else "50%"

    return jsonify({
        "last_packet": str(last_packet),
        "packets_today": packets_today,
        "reporting_interval": reporting_interval,
        "data_quality": data_quality
    })




@app.route("/device_alarm_summary/<int:device_id>")
def device_alarm_summary(device_id):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT device_eui
        FROM devices
        WHERE id = %s
    """, (device_id,))

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return jsonify({
            "total": 0,
            "critical": 0,
            "warning": 0
        })

    device_eui = row[0]

    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE severity = 'CRITICAL'),
            COUNT(*) FILTER (WHERE severity = 'WARNING')
        FROM active_alarms
        WHERE device_eui = %s
    """, (device_eui,))

    stats = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({
        "total": stats[0],
        "critical": stats[1],
        "warning": stats[2]
    })


@app.route("/device_active_alarms/<int:device_id>")
def device_active_alarms(device_id):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT device_eui
    FROM devices
    WHERE id = %s
    """, (device_id,))

    device = cursor.fetchone()

    if not device:
        cursor.close()
        conn.close()
        return jsonify({"error": "Device not found"}), 404

    device_eui = device[0]

    cursor.execute("""
    SELECT
        id,
        parameter,
        alarm_reason,
        severity,
        alarm_status,
        first_seen,
        last_seen
    FROM active_alarms
    WHERE device_eui = %s
    AND alarm_status IN ('OPEN','ACKNOWLEDGED')
    ORDER BY id DESC
    """, (device_eui,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify([
        {
            "id": r[0],
            "parameter": r[1],
            "alarm_reason": r[2],
            "severity": r[3],
            "alarm_status": r[4],
            "first_seen": str(r[5]),
            "last_seen": str(r[6])
        }
        for r in rows
    ])



@app.route("/device_telemetry_history/<int:device_id>")
def device_telemetry_history(device_id):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT device_eui
    FROM devices
    WHERE id = %s
    """, (device_id,))

    device = cursor.fetchone()

    if not device:
        cursor.close()
        conn.close()
        return jsonify({"error": "Device not found"}), 404

    device_eui = device[0]

    cursor.execute("""
    SELECT
        timestamp,
        payload
    FROM sensor_data
    WHERE device_eui = %s
    ORDER BY timestamp DESC
    LIMIT 100
    """, (device_eui,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    data = []

    for r in reversed(rows):
        data.append({
            "timestamp": str(r[0]),
            "payload": r[1]
        })

    return jsonify(data)

@app.route("/device_details_page/<int:device_id>")
def device_details_page(device_id):
    if "user" not in session:
        return redirect("/login")

    return render_template(
        "device_details.html",
        device_id=device_id
    )



@app.route("/device_details/<int:device_id>")
def device_details(device_id):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        d.id,
        d.device_eui,
        d.device_name,
        d.device_type,
        d.site,
        d.asset_id,
        a.asset_name,
        a.asset_type
    FROM devices d
    LEFT JOIN assets a
        ON d.asset_id = a.id
    WHERE d.id = %s
    """, (device_id,))

    device = cursor.fetchone()

    if not device:
        cursor.close()
        conn.close()
        return jsonify({"error": "Device not found"}), 404

    device_eui = device[1]

    cursor.execute("""
    SELECT
        timestamp,
        temp,
        payload
    FROM sensor_data
    WHERE device_eui = %s
    ORDER BY timestamp DESC
    LIMIT 1
    """, (device_eui,))

    latest = cursor.fetchone()

    cursor.execute("""
    SELECT
        id,
        parameter,
        alarm_reason,
        severity,
        alarm_status,
        first_seen,
        last_seen
    FROM active_alarms
    WHERE device_eui = %s
    ORDER BY id DESC
    LIMIT 10
    """, (device_eui,))

    alarms = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify({
        "device": {
            "id": device[0],
            "device_eui": device[1],
            "device_name": device[2],
            "device_type": device[3],
            "site": device[4],
            "asset_id": device[5],
            "asset_name": device[6],
            "asset_type": device[7]
        },
        "latest_telemetry": {
            "timestamp": str(latest[0]) if latest else None,
            "temperature": latest[1] if latest else None,
            "payload": latest[2] if latest else None
        },
        "alarms": [
            {
                "id": a[0],
                "parameter": a[1],
                "alarm_reason": a[2],
                "severity": a[3],
                "alarm_status": a[4],
                "first_seen": str(a[5]),
                "last_seen": str(a[6])
            }
            for a in alarms
        ]
    })




@app.route("/device_status_page")
def device_status_page():
    if "user" not in session:
        return redirect("/login")

    return render_template("device_status.html")


@app.route("/device_status")
def device_status():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        d.id,
        d.device_eui,
        d.device_name,
        d.device_type,
        d.site,
        d.asset_id,
        MAX(s.timestamp) AS last_seen
    FROM devices d
    LEFT JOIN sensor_data s
        ON d.device_eui = s.device_eui
    WHERE d.is_active = 1
    GROUP BY
        d.id,
        d.device_eui,
        d.device_name,
        d.device_type,
        d.site,
        d.asset_id
    ORDER BY d.id DESC
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    devices = []

    for r in rows:
        last_seen = r[6]

        if last_seen is None:
            status = "NEVER_SEEN"
        else:
            from datetime import datetime
            age_seconds = (datetime.now() - last_seen).total_seconds()

            if age_seconds <= 120:
                status = "ONLINE"
            elif age_seconds <= 3600:
                status = "STALE"
            else:
                status = "OFFLINE"

        devices.append({
            "id": r[0],
            "device_eui": r[1],
            "device_name": r[2],
            "device_type": r[3],
            "site": r[4],
            "asset_id": r[5],
            "last_seen": str(last_seen) if last_seen else "",
            "status": status
        })

    return jsonify(devices)




@app.route("/dashboard_summary")
def dashboard_summary():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM sites")
    total_sites = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM devices WHERE is_active = 1")
    total_devices = cursor.fetchone()[0]

    cursor.execute("""
    SELECT COUNT(*)
    FROM active_alarms
    WHERE alarm_status IN ('OPEN', 'ACKNOWLEDGED')
    """)
    active_alarms_count = cursor.fetchone()[0]

    cursor.execute("""
    SELECT COUNT(DISTINCT device_eui)
    FROM sensor_data
    WHERE timestamp >= NOW() - INTERVAL '2 minutes'
    """)
    online_devices = cursor.fetchone()[0]

    offline_devices = total_devices - online_devices

    cursor.close()
    conn.close()

    return jsonify({
        "total_sites": total_sites,
        "total_devices": total_devices,
        "online_devices": online_devices,
        "offline_devices": offline_devices,
        "active_alarms": active_alarms_count
    })





@app.route("/alarm_history_page")
def alarm_history_page():
    if "user" not in session:
        return redirect("/login")

    return render_template("alarm_history.html")



@app.route("/alarm_history_full")
def alarm_history_full():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        id,
        device_eui,
        parameter,
        alarm_reason,
        severity,
        alarm_status,
        first_seen,
        last_seen,
        cleared_at
    FROM active_alarms
    ORDER BY id DESC
    LIMIT 500
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    data = []

    for r in rows:
        data.append({
            "id": r[0],
            "device_eui": r[1],
            "parameter": r[2],
            "alarm_reason": r[3],
            "severity": r[4],
            "alarm_status": r[5],
            "first_seen": str(r[6]),
            "last_seen": str(r[7]),
            "cleared_at": str(r[8]) if r[8] else ""
        })

    return jsonify(data)




@app.route("/active_alarms_page")
def active_alarms_page():

    if "user" not in session:
        return redirect("/login")

    return render_template(
        "active_alarms.html",
        role=session.get("role", "user")
    )






@app.route("/active_alarms/<int:alarm_id>/acknowledge", methods=["POST"])
def acknowledge_active_alarm(alarm_id):
    user = session.get("user", "admin")

    success = acknowledge_active_alarm_pg(
        alarm_id,
        user
    )

    if not success:
        return jsonify({
            "error": "Alarm not found or not OPEN"
        }), 404

    return jsonify({
        "message": "Alarm acknowledged successfully",
        "alarm_id": alarm_id,
        "acknowledged_by": user
    })




@app.route("/active_alarms")
def active_alarms():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        id,
        device_eui,
        parameter,
        alarm_reason,
        severity,
        alarm_status,
        first_seen,
        last_seen,
        cleared_at
    FROM active_alarms
    WHERE alarm_status IN ('OPEN','ACKNOWLEDGED')
    ORDER BY severity DESC,
        first_seen ASC;
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    alarms = []

    for r in rows:
        alarms.append({
            "id": r[0],
            "device_eui": r[1],
            "parameter": r[2],
            "alarm_reason": r[3],
            "severity": r[4],
            "alarm_status": r[5],
            "first_seen": str(r[6]),
            "last_seen": str(r[7]),
            "cleared_at": str(r[8]) if r[8] else None
        })

    return jsonify(alarms)


@app.route("/devices_by_asset/<int:asset_id>")
def devices_by_asset(asset_id):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        device_eui,
        device_name,
        device_type
    FROM devices
    WHERE asset_id = %s
    AND is_active = 1
    ORDER BY device_name
    """, (asset_id,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    devices = []

    for r in rows:
        devices.append({
            "device_eui": r[0],
            "device_name": r[1],
            "device_type": r[2]
        })

    return jsonify(devices)




@app.route("/assets_by_site/<site_name>")
def assets_by_site_name(site_name):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        a.id,
        a.asset_name,
        a.asset_type
    FROM assets a
    JOIN sites s ON a.site_id = s.id
    WHERE LOWER(s.name) = LOWER(%s)
    AND a.is_active = 1
    ORDER BY a.asset_name
    """, (site_name,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    assets = []

    for row in rows:
        assets.append({
            "asset_id": row[0],
            "asset_name": row[1],
            "asset_type": row[2]
        })

    return jsonify(assets)










@app.route("/sites_registry/<int:site_id>", methods=["PUT"])
def update_site_registry(site_id):
    data = request.get_json()

    
    site_name = data.get("site_name", "").strip()

    if not site_name:
        return jsonify({"error": "site_name is required"}), 400

    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE sites
    SET name = %s
    WHERE id = %s
    RETURNING id
    """, (site_name, site_id))

    updated = cursor.fetchone()

    if not updated:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({"error": "Site not found"}), 404

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({
        "message": "Site updated successfully",
        "site_id": site_id
    })







@app.route("/sites_registry/<int:site_id>", methods=["DELETE"])
def delete_site_registry(site_id):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    DELETE FROM sites
    WHERE id = %s
    RETURNING id
    """, (site_id,))

    deleted = cursor.fetchone()

    if not deleted:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({"error": "Site not found"}), 404

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({
        "message": "Site deleted successfully",
        "site_id": site_id
    })



@app.route("/sites_registry", methods=["POST"])
def add_site_registry():
    data = request.get_json()


    site_name = data.get("site_name", "").strip()

    if not site_name:
        return jsonify({"error": "site_name is required"}), 400

    conn = get_pg_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO sites (name)
        VALUES (%s)
        RETURNING id
        """, (site_name,))

        new_id = cursor.fetchone()[0]
        conn.commit()

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({"error": str(e)}), 400

    cursor.close()
    conn.close()

    return jsonify({
        "message": "Site added successfully",
        "site_id": new_id
    }), 201




@app.route("/site_registry_page")
def site_registry_page():

    check = require_role("admin")
    if check:
        return check

    return render_template("site_registry.html")




@app.route("/asset_registry_page")
def asset_registry_page():
    check = require_role("admin")
    if check:
        return check

    return render_template("asset_registry.html")




@app.route("/assets_registry/<int:asset_id>", methods=["DELETE"])
def deactivate_asset_registry(asset_id):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE assets
    SET is_active = 0
    WHERE id = %s
    RETURNING id
    """, (asset_id,))

    deleted = cursor.fetchone()

    if not deleted:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({"error": "Asset not found"}), 404

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({
        "message": "Asset deactivated successfully",
        "asset_id": asset_id
    })





@app.route("/assets_registry/<int:asset_id>", methods=["PUT"])
def update_asset_registry(asset_id):
    data = request.get_json()

    site_id = data.get("site_id")
    asset_name = data.get("asset_name")
    asset_type = data.get("asset_type")

    if not site_id or not asset_name or not asset_type:
        return jsonify({
            "error": "site_id, asset_name, and asset_type are required"
        }), 400

    conn = get_pg_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        UPDATE assets
        SET
            site_id = %s,
            asset_name = %s,
            asset_type = %s
        WHERE id = %s
        RETURNING id
        """, (
            site_id,
            asset_name,
            asset_type,
            asset_id
        ))

        updated = cursor.fetchone()

        if not updated:
            conn.rollback()
            cursor.close()
            conn.close()
            return jsonify({"error": "Asset not found"}), 404

        conn.commit()

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({"error": str(e)}), 400

    cursor.close()
    conn.close()

    return jsonify({
        "message": "Asset updated successfully",
        "asset_id": asset_id
    })






@app.route("/assets_registry", methods=["POST"])
def add_asset_registry():
    data = request.get_json()

    site_id = data.get("site_id")
    asset_name = data.get("asset_name")
    asset_type = data.get("asset_type")

    if not site_id or not asset_name or not asset_type:
        return jsonify({
            "error": "site_id, asset_name, and asset_type are required"
        }), 400

    conn = get_pg_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO assets (
            site_id,
            asset_name,
            asset_type
        )
        VALUES (%s, %s, %s)
        RETURNING id
        """, (
            site_id,
            asset_name,
            asset_type
        ))

        new_id = cursor.fetchone()[0]
        conn.commit()

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({"error": str(e)}), 400

    cursor.close()
    conn.close()

    return jsonify({
        "message": "Asset added successfully",
        "asset_id": new_id
    }), 201





@app.route("/assets_registry_full")
def assets_registry_full():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
        a.id,
        s.name AS site,
        a.asset_name,
        a.asset_type,
        a.is_active
    FROM assets a
    LEFT JOIN sites s ON a.site_id = s.id
    ORDER BY a.id DESC
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    assets = []

    for r in rows:
        assets.append({
            "asset_id": r[0],
            "site": r[1],
            "asset_name": r[2],
            "asset_type": r[3],
            "is_active": r[4]
        })
        

    return jsonify(assets)




@app.route("/device_registry_page")
def device_registry_page():
    check = require_role("admin")
    if check:
        return check

    return render_template("device_registry.html")






@app.route("/sites_registry")
def sites_registry():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT id, name
    FROM sites
    ORDER BY name
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    sites = []

    for r in rows:
        sites.append({
            "site_id": r[0],
            "site_name": r[1]
        })

    return jsonify(sites)




@app.route("/assets_registry")
def assets_registry():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        a.id,
        s.name AS site,
        a.asset_name,
        a.asset_type
    FROM assets a
    JOIN sites s ON a.site_id = s.id
    ORDER BY s.name, a.asset_name
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    assets = []

    for r in rows:
        assets.append({
            "asset_id": r[0],
            "site": r[1],
            "asset_name": r[2],
            "asset_type": r[3]
        })

    return jsonify(assets)






@app.route("/devices_registry", methods=["POST"])
def add_device_registry():
    data = request.get_json()

    device_eui = data.get("device_eui")
    device_name = data.get("device_name")
    device_type = data.get("device_type")
    site = data.get("site")
    asset_id = data.get("asset_id")
    is_active = data.get("is_active", 1)

    if not device_eui or not device_name or not device_type:
        return jsonify({
            "error": "device_eui, device_name, and device_type are required"
        }), 400

    conn = get_pg_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO devices (
            device_eui,
            device_name,
            device_type,
            site,
            asset_id,
            is_active
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """, (
            device_eui,
            device_name,
            device_type,
            site,
            asset_id,
            is_active
        ))

        new_id = cursor.fetchone()[0]
        conn.commit()

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()

        return jsonify({
            "error": str(e)
        }), 400

    cursor.close()
    conn.close()

    return jsonify({
        "message": "Device added successfully",
        "device_id": new_id
    }), 201




@app.route("/devices_registry/<int:device_id>", methods=["PUT"])
def update_device_registry(device_id):
    data = request.get_json()

    conn = get_pg_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        UPDATE devices
        SET
            device_eui = %s,
            device_name = %s,
            device_type = %s,
            site = %s,
            asset_id = %s,
            is_active = %s
        WHERE id = %s
        RETURNING id
        """, (
            data.get("device_eui"),
            data.get("device_name"),
            data.get("device_type"),
            data.get("site"),
            data.get("asset_id"),
            data.get("is_active", 1),
            device_id
        ))

        updated = cursor.fetchone()

        if not updated:
            conn.rollback()
            cursor.close()
            conn.close()
            return jsonify({"error": "Device not found"}), 404

        conn.commit()

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({"error": str(e)}), 400

    cursor.close()
    conn.close()

    return jsonify({
        "message": "Device updated successfully",
        "device_id": device_id
    })


@app.route("/devices_registry/<int:device_id>", methods=["DELETE"])
def deactivate_device_registry(device_id):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE devices
    SET is_active = 0
    WHERE id = %s
    RETURNING id
    """, (device_id,))

    deleted = cursor.fetchone()

    if not deleted:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify({"error": "Device not found"}), 404

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({
        "message": "Device deactivated successfully",
        "device_id": device_id
    })



@app.route("/devices_registry")
def devices_registry():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        id,
        device_eui,
        device_name,
        device_type,
        site,
        asset_id,
        is_active,
        created_at
    FROM devices
    ORDER BY id DESC
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    devices = []

    for r in rows:
        devices.append({
            "id": r[0],
            "device_eui": r[1],
            "device_name": r[2],
            "device_type": r[3],
            "site": r[4],
            "asset_id": r[5],
            "is_active": r[6],
            "created_at": str(r[7])
        })

    return jsonify(devices)






def create_offline_incidents(timeout_seconds=60):
    conn = sqlite3.connect("iot.db", timeout=10)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        sd.device_eui,
        MAX(sd.timestamp) as last_seen
    FROM sensor_data sd
    JOIN asset_devices ad
        ON TRIM(sd.device_eui) = TRIM(ad.device_eui)
    WHERE ad.is_active = 1
    GROUP BY sd.device_eui
    """)

    rows = cursor.fetchall()
    now = datetime.now()

    for device_eui, last_seen in rows:
        try:
            last_time = datetime.fromisoformat(last_seen)
            diff = int((now - last_time).total_seconds())

            if diff > timeout_seconds:

                already_exists = recent_anomaly_exists(
                    device_eui,
                    "HIGH",
                    minutes=10
                )


                if not already_exists:
                    insert_anomaly_event(
                        "HEARTBEAT",
                        device_eui,
                        "device_heartbeat",
                        100,
                        "HIGH",
                        f"Device offline. No telemetry for {diff} seconds"
                    )
                    if not recent_anomaly_exists_pg(device_eui, "HIGH", minutes=10):
                        insert_anomaly_event_pg(
                            "HEARTBEAT",
                            device_eui,
                            "device_heartbeat",
                            100,
                            "HIGH",
                            f"Device offline. No telemetry for {diff} seconds"
                        )
                
        except Exception as e:
            print("Offline incident error:", e)

    conn.close()





@app.route("/device_heartbeat_status")
def device_heartbeat_status():
    create_offline_incidents(timeout_seconds=60)

    conn = get_pg_connection()
    cursor = conn.cursor()


    cursor.execute("""
    SELECT
        sd.device_eui,
        MAX(sd.timestamp) as last_seen
    FROM sensor_data sd
    JOIN asset_devices ad
        ON TRIM(sd.device_eui) = TRIM(ad.device_eui)
    WHERE ad.is_active = 1
    GROUP BY sd.device_eui
    ORDER BY last_seen DESC
    """)
    
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    devices = []

    now = datetime.now()

    for r in rows:
        device_eui = r[0]
        last_seen = r[1]

        try:
            last_time = last_seen
            diff_seconds = int((now - last_time).total_seconds())

            status = "ONLINE" if diff_seconds <= 60 else "OFFLINE"
            if status == "ONLINE":
                resolve_open_incidents_pg(device_eui)

        except Exception:
            diff_seconds = None
            status = "UNKNOWN"

        devices.append({
            "device_eui": device_eui,
            "last_seen": last_seen,
            "seconds_since_seen": diff_seconds,
            "status": status
        })

    return jsonify(devices)



@app.route("/incident_timeline")
def incident_timeline():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        timestamp,
        device_eui,
        anomaly_level,
        anomaly_score,
        anomaly_reason,
        incident_status
    FROM anomaly_events
    ORDER BY timestamp DESC
    LIMIT 25
    """)

    anomalies = cursor.fetchall()

    cursor.execute("""
    SELECT
        acknowledged_at,
        device_eui,
        acknowledged_by,
        alarm_message
    FROM alarm_acknowledgements
    ORDER BY acknowledged_at DESC
    LIMIT 25
    """)

    acknowledgements = cursor.fetchall()
    cursor.close()
    conn.close()

    timeline = []

    for a in anomalies:
        timeline.append({
            "time": str(a[0]),
            "type": "AI DETECTION",
            "device": a[1],
            "level": a[2],
            "score": a[3],
            "message": a[4],
            "status": a[5]
        })
    for ack in acknowledgements:
        timeline.append({
            "time": str(ack[0]),
            "type": "ACKNOWLEDGED",
            "device": ack[1],
            "level": "INFO",
            "score": "-",
            "message": f"{ack[2]} acknowledged: {ack[3]}"
        })

    timeline.sort(
        key=lambda x: x["time"],
        reverse=True
    )

    return jsonify(timeline[:50])


def generate_ai_recommendation(anomaly_reasons, temperature=None, humidity=None, battery=None):

    recommendations = []

    reasons_text = ", ".join(anomaly_reasons or [])

    if "High temperature" in reasons_text:
        recommendations.append("Check cooling fan or ventilation.")
        recommendations.append("Inspect generator room airflow.")
        recommendations.append("Confirm load is not above safe operating range.")

    if "Device offline" in reasons_text:
        recommendations.append("Check device power supply.")
        recommendations.append("Verify network or Modbus communication.")
        recommendations.append("Inspect gateway connection.")

    if "battery" in reasons_text.lower():
        recommendations.append("Inspect battery level and replace if weak.")

    if humidity is not None and humidity > 80:
        recommendations.append("Check for moisture or poor environmental control.")

    if not recommendations:
        recommendations.append("Continue monitoring. No immediate maintenance action required.")

    return {
        "ai_recommendations": recommendations
    }




@app.route("/anomaly_score_trend")
def anomaly_score_trend():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT timestamp, anomaly_score, anomaly_level
    FROM anomaly_events
    ORDER BY timestamp DESC
    LIMIT 50
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    rows = rows[::-1]

    data = {
        "timestamps": [],
        "scores": [],
        "levels": []
    }

    for r in rows:
        data["timestamps"].append(str(r[0]))
        data["scores"].append(r[1])
        data["levels"].append(r[2])

    return jsonify(data)



@app.route("/alarm_history")
def alarm_history():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        device_eui,
        alarm_message,
        acknowledged_by,
        acknowledged_at
    FROM alarm_acknowledgements
    ORDER BY acknowledged_at DESC
    LIMIT 50
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    alarms = []

    for r in rows:
        alarms.append({
            "device_eui": r[0],
            "alarm_message": r[1],
            "acknowledged_by": r[2],
            "acknowledged_at": str(r[3])
        })

    return jsonify(alarms)



@app.route("/acknowledge_alarm", methods=["POST"])
def acknowledge_alarm():
    data = request.get_json()

    device_eui = data.get("device_eui", "UNKNOWN")
    alarm_message = data.get("alarm_message", "UNKNOWN")
    acknowledged_by = session.get("user", "UNKNOWN")

    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO alarm_acknowledgements (
        device_eui,
        alarm_message,
        acknowledged_by
    )
    VALUES (%s, %s, %s)
    """, (device_eui, alarm_message, acknowledged_by))

    cursor.execute("""
    UPDATE anomaly_events
    SET incident_status = 'ACKNOWLEDGED'
    WHERE id = (
        SELECT id
        FROM anomaly_events
        WHERE device_eui = %s
        AND incident_status = 'OPEN'
        ORDER BY timestamp DESC
        LIMIT 1
    )
    """, (device_eui,))

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"status": "acknowledged"})


@app.route("/recent_incidents")
def recent_incidents():
    conn = get_pg_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT
        timestamp,
        device_eui,
        anomaly_level,
        anomaly_score,
        anomaly_reason
    FROM anomaly_events
    ORDER BY timestamp DESC
    LIMIT 20
    """)
    
    rows = cursor.fetchall()
    conn.close()

    incidents = []

    for r in rows:
        incidents.append({
            "timestamp": str(r[0]),
            "device_eui": r[1],
            "level": r[2],
            "score": r[3],
            "reason": r[4]
        })

    return jsonify(incidents)





def calculate_device_reliability(device_eui):
    conn = sqlite3.connect("iot.db", timeout=10)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT COUNT(*), MAX(timestamp)
    FROM anomaly_events
    WHERE device_eui = ?
    """, (device_eui,))

    total_anomalies, last_incident = cursor.fetchone()
    conn.close()

    score = 100 - (total_anomalies * 5)

    if score < 0:
        score = 0

    if score >= 80:
        level = "GOOD"
    elif score >= 50:
        level = "WATCH"
    else:
        level = "POOR"

    return {
        "device_eui": device_eui,
        "reliability_score": score,
        "reliability_level": level,
        "total_anomalies": total_anomalies,
        "last_anomaly": last_incident
    }





@app.route("/asset_health")
def asset_health():
    asset_id = request.args.get("asset_id")

    if not asset_id:
        return jsonify({"error": "asset_id is required"}), 400

    conn = sqlite3.connect("iot.db", timeout=10)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT device_eui
    FROM asset_devices
    WHERE asset_id = ?
    AND is_active = 1
    LIMIT 1
    """, (asset_id,))

    row = cursor.fetchone()
    conn.close()

    if not row:
        return jsonify({
            "reliability_score": "--",
            "reliability_level": "--",
            "total_anomalies": "--",
            "last_anomaly": "--"
        })

    return jsonify(calculate_device_reliability(row[0]))






@app.route("/asset_temperature_data")
def asset_temperature_data():
    asset_id = request.args.get("asset_id")

    if not asset_id:
        return jsonify({"error": "asset_id is required"}), 400

    
    rows = get_data_by_asset_pg(asset_id)
    rows = rows[::-1]

    data = {
        "timestamps": [],
        "event": [],
        "state": [],
        "battery": [],
        "movement": [],
        "temperature": [],
        "humidity": []
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
            print("Asset temp data error:", e)

    if len(data["timestamps"]) > 0:
        anomaly = analyze_temperature(
            temperature=data["temperature"][-1],
            humidity=data["humidity"][-1],
            battery=data["battery"][-1],
            status="ONLINE",
            temp_history=data["temperature"]
        )
    else:
        anomaly = analyze_temperature(status="OFFLINE")

    data.update(anomaly)

    recommendation = generate_ai_recommendation(
        anomaly.get("anomaly_reasons", []),
        temperature=data["temperature"][-1] if data["temperature"] else None,
        humidity=data["humidity"][-1] if data["humidity"] else None,
        battery=data["battery"][-1] if data["battery"] else None
    )

    data.update(recommendation)

    prediction = predict_temperature_trend(data["temperature"])
    data.update(prediction)
    if anomaly["anomaly_level"] == "NORMAL":

        conn = sqlite3.connect("iot.db", timeout=10)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT device_eui
        FROM asset_devices
        WHERE asset_id = ?
        AND is_active = 1
        LIMIT 1
        """, (asset_id,))

        row = cursor.fetchone()

        if row:
            device_eui = row[0]

            cursor.execute("""
            UPDATE anomaly_events
            SET incident_status = 'RESOLVED',
                resolved_at = CURRENT_TIMESTAMP
            WHERE device_eui = ?
            AND incident_status IN ('OPEN', 'ACKNOWLEDGED')
            """, (device_eui,))

        conn.commit()
        conn.close()

    # if anomaly["anomaly_level"] in ["LOW", "MEDIUM", "HIGH"]:

    #     conn = sqlite3.connect("iot.db", timeout=10)
    #     cursor = conn.cursor()

    #     cursor.execute("""
    #     SELECT device_eui
    #     FROM asset_devices
    #     WHERE asset_id = ?
    #     AND is_active = 1
    #     LIMIT 1
    #     """, (asset_id,))

    #     row = cursor.fetchone()
    #     conn.close()

    #     if row:
    #         device_eui = row[0]
    #         reason = ", ".join(anomaly["anomaly_reasons"])
            
    #         if not recent_anomaly_exists_pg(device_eui, anomaly["anomaly_level"], minutes=5):
    #             insert_anomaly_event_pg(
    #             "ASSET_MODE",
    #             device_eui,
    #             "asset_temperature_sensor",
    #             anomaly["anomaly_score"],
    #             anomaly["anomaly_level"],
    #             reason
    #         )

    #         already_exists = recent_anomaly_exists(
    #             device_eui,
    #             anomaly["anomaly_level"],
    #             minutes=5
    #         )

    #         if not already_exists:
    #             insert_anomaly_event(
    #                 "ASSET_MODE",
    #                 device_eui,
    #                 "asset_temperature_sensor",
    #                 anomaly["anomaly_score"],
    #                 anomaly["anomaly_level"],
    #                 reason
    #             )

    # return jsonify(data)
    
    
    if anomaly["anomaly_level"] in ["LOW", "MEDIUM", "HIGH"]:

        conn = get_pg_connection()
        cursor = conn.cursor()

        cursor.execute("""
        SELECT device_eui
        FROM asset_devices
        WHERE asset_id = %s
        AND is_active = 1
        LIMIT 1
        """, (asset_id,))

        row = cursor.fetchone()

        cursor.close()
        conn.close()

        if row:
            device_eui = row[0]

            reason = ", ".join(anomaly["anomaly_reasons"])

            if not recent_anomaly_exists_pg(
                device_eui,
                anomaly["anomaly_level"],
                minutes=5
            ):
                insert_anomaly_event_pg(
                    "ASSET_MODE",
                    device_eui,
                    "asset_temperature_sensor",
                    anomaly["anomaly_score"],
                    anomaly["anomaly_level"],
                    reason
                )

    return jsonify(data)





@app.route("/assets_by_site")
def assets_by_site():
    site = request.args.get("site")

    if not site:
        return jsonify([])

    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        a.id,
        a.asset_name,
        a.asset_type
    FROM assets a
    JOIN sites s ON a.site_id = s.id
    WHERE LOWER(s.name) = LOWER(%s)
    ORDER BY a.asset_name
    """, (site,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    assets = []

    for r in rows:
        assets.append({
            "asset_id": r[0],
            "asset_name": r[1],
            "asset_type": r[2]
        })

    return jsonify(assets)

# ---------------- ROUTES ----------------
@app.route("/device_health")
def device_health():
    check = require_role("admin", "operator", "viewer")
    if check:
        return check

    device_eui = request.args.get("device_eui")

    if not device_eui:
        return jsonify({"error": "device_eui is required"}), 400

    stats = get_device_anomaly_stats(device_eui)

    anomaly_count = stats["total_anomalies"]

    reliability_score = max(0, 100 - (anomaly_count * 5))

    if reliability_score >= 80:
        reliability_level = "GOOD"
    elif reliability_score >= 50:
        reliability_level = "WATCH"
    else:
        reliability_level = "POOR"

    return jsonify({
        "device_eui": device_eui,
        "total_anomalies": anomaly_count,
        "last_anomaly": stats["last_anomaly"],
        "reliability_score": reliability_score,
        "reliability_level": reliability_level
    })




@app.route("/anomaly_history")
def anomaly_history():
    check = require_role("admin", "operator", "viewer")
    if check:
        return check

    rows = get_recent_anomaly_events()

    data = []
    for r in rows:
        data.append({
            "site": r[0],
            "device_eui": r[1],
            "device_type": r[2],
            "score": r[3],
            "level": r[4],
            "reason": r[5],
            "timestamp": r[6]
        })

    return jsonify(data)





@app.route("/api/telemetry", methods=["POST"])
def api_telemetry():
    # api_key = request.headers.get("X-API-Key")
    # if api_key != API_KEY:
    #     return jsonify({"error": "Unauthorized"}), 401

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
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        site,
        device_name,
        device_type,
        device_eui
    FROM devices
    WHERE is_active = 1
    ORDER BY site, device_name
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

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
            status=data["status"],
            temp_history=data["temperature"]
)
    else:
        anomaly = analyze_temperature(status="OFFLINE")

    data.update(anomaly)
    prediction = predict_temperature_trend(data["temperature"])
    data.update(prediction)


    if anomaly["anomaly_level"] == "HIGH":

        
        already_exists = recent_anomaly_exists(
            device_eui,
            anomaly["anomaly_level"],
            minutes=5
        )
        
        
        

        if not already_exists:
            insert_anomaly_event(
                site,
                device_eui,
                "temperature_sensor",
                anomaly["anomaly_score"],
                anomaly["anomaly_level"],
                ", ".join(anomaly["anomaly_reasons"])
            )
            reason = ", ".join(anomaly["anomaly_reasons"])
            if not recent_anomaly_exists_pg(device_eui, anomaly["anomaly_level"], minutes=5):
                 insert_anomaly_event_pg(
                    "HEARTBEAT",
                    device_eui,
                    "device_heartbeat",
                    100,
                    "HIGH",
                    f"Device offline. No telemetry for {diff} seconds"
                )
            if not recent_anomaly_exists_pg(device_eui, anomaly["anomaly_level"], minutes=5):
                insert_anomaly_event_pg(
                    "ASSET_MODE",
                    device_eui,
                    "asset_temperature_sensor",
                    anomaly["anomaly_score"],
                    anomaly["anomaly_level"],
                    reason
                )

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
