import time
import random
import requests
from postgres_db import get_pg_connection


def emit_command_update(device_id, command_name, status):
    try:
        requests.post(
            "http://flask_app:5000/emit_device_command_update",
            json={
                "device_id": device_id,
                "command": command_name,
                "status": status
            },
            timeout=3
        )
    except Exception as e:
        print("Failed to emit socket update:", e)


print("IoT device command worker started...")

while True:
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, device_id, command_name
        FROM iot_device_commands
        WHERE command_status='PENDING'
        ORDER BY id ASC
        LIMIT 1
    """)

    command = cur.fetchone()

    if command:
        command_id, device_id, command_name = command

        print("Processing device command:", command_id, command_name)

        # -----------------------------
        # Update to RUNNING
        # -----------------------------
        cur.execute("""
            UPDATE iot_device_commands
            SET command_status='RUNNING'
            WHERE id=%s
        """, (command_id,))
        conn.commit()

        # ✅ Emit RUNNING event
        emit_command_update(device_id, command_name, "RUNNING")

        time.sleep(5)

        success = random.choice([True, True, True, False])

        if success:

            # -----------------------------
            # Update to SUCCESS
            # -----------------------------
            cur.execute("""
                UPDATE iot_device_commands
                SET command_status='SUCCESS',
                    completed_at=CURRENT_TIMESTAMP
                WHERE id=%s
            """, (command_id,))

            conn.commit()

            # ✅ Emit SUCCESS event
            emit_command_update(device_id, command_name, "SUCCESS")

        else:

            # -----------------------------
            # Update to FAILED
            # -----------------------------
            cur.execute("""
                UPDATE iot_device_commands
                SET command_status='FAILED',
                    completed_at=CURRENT_TIMESTAMP
                WHERE id=%s
            """, (command_id,))

            conn.commit()

            # ✅ Emit FAILED event
            emit_command_update(device_id, command_name, "FAILED")

    cur.close()
    conn.close()

    time.sleep(2)