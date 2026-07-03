import time
import random
import requests
from postgres_db import get_pg_connection


def emit_firmware_update(device_id, status, progress):
    try:
        requests.post(
            "http://flask_app:5000/emit_firmware_update",
            json={
                "device_id": device_id,
                "status": status,
                "progress": progress
            },
            timeout=3
        )
    except Exception as e:
        print("Socket emit failed:", e)


print("Firmware update worker started...")

while True:
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, device_id, target_version
        FROM iot_device_firmware_updates
        WHERE update_status='PENDING'
        ORDER BY id ASC
        LIMIT 1
    """)

    update = cur.fetchone()

    if update:
        update_id, device_id, target_version = update

        cur.execute("""
            UPDATE iot_device_firmware_updates
            SET update_status='RUNNING',
                started_at=CURRENT_TIMESTAMP,
                progress=10
            WHERE id=%s
        """, (update_id,))
        conn.commit()

        emit_firmware_update(device_id, "RUNNING", 10)

        for progress in [25, 50, 75, 100]:
            time.sleep(3)

            cur.execute("""
                UPDATE iot_device_firmware_updates
                SET progress=%s
                WHERE id=%s
            """, (progress, update_id))
            conn.commit()

            emit_firmware_update(device_id, "RUNNING", progress)

        success = random.choice([True, True, True, False])

        if success:
            cur.execute("""
                UPDATE iot_device_firmware_updates
                SET update_status='SUCCESS',
                    completed_at=CURRENT_TIMESTAMP,
                    progress=100
                WHERE id=%s
            """, (update_id,))

            cur.execute("""
                UPDATE iot_devices
                SET firmware_version=%s
                WHERE id=%s
            """, (target_version, device_id))

            conn.commit()

            emit_firmware_update(device_id, "SUCCESS", 100)

        else:
            cur.execute("""
                UPDATE iot_device_firmware_updates
                SET update_status='FAILED',
                    completed_at=CURRENT_TIMESTAMP,
                    error_message='Simulated OTA update failure'
                WHERE id=%s
            """, (update_id,))

            conn.commit()

            emit_firmware_update(device_id, "FAILED", progress)

    cur.close()
    conn.close()

    time.sleep(2)