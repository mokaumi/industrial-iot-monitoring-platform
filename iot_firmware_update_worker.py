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
    conn = None
    cur = None

    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        ####################################################
        # Find the next pending firmware job
        ####################################################

        cur.execute("""
            SELECT
                id,
                device_id,
                current_version,
                target_version,
                update_type
            FROM iot_device_firmware_updates
            WHERE update_status='PENDING'
            ORDER BY id ASC
            LIMIT 1
        """)

        update = cur.fetchone()

        if update:
            (
                update_id,
                device_id,
                current_version,
                target_version,
                update_type
            ) = update

            # Protect old records in case update_type is NULL
            update_type = (
                update_type
                or "FIRMWARE_UPDATE"
            )

            print(
                f"Processing firmware job {update_id}: "
                f"device={device_id}, "
                f"type={update_type}, "
                f"{current_version} -> {target_version}"
            )

            ################################################
            # Mark firmware job RUNNING
            ################################################

            cur.execute("""
                UPDATE iot_device_firmware_updates
                SET
                    update_status='RUNNING',
                    started_at=CURRENT_TIMESTAMP,
                    progress=10,
                    error_message=NULL
                WHERE id=%s
            """, (
                update_id,
            ))

            ################################################
            # Mark rollback history RUNNING
            ################################################

            if update_type == "ROLLBACK":
                cur.execute("""
                    UPDATE iot_firmware_rollbacks
                    SET
                        rollback_status='RUNNING',
                        started_at=CURRENT_TIMESTAMP,
                        error_message=NULL
                    WHERE rollback_update_id=%s
                """, (
                    update_id,
                ))

            conn.commit()

            emit_firmware_update(
                device_id,
                "RUNNING",
                10
            )

            ################################################
            # Simulated OTA progress
            ################################################

            for progress in [25, 50, 75, 100]:
                time.sleep(3)

                cur.execute("""
                    UPDATE iot_device_firmware_updates
                    SET progress=%s
                    WHERE id=%s
                """, (
                    progress,
                    update_id
                ))

                conn.commit()

                emit_firmware_update(
                    device_id,
                    "RUNNING",
                    progress
                )

            ################################################
            # Simulated result
            ################################################

            success = random.choice([
                True,
                True,
                True,
                False
            ])

            if success:

                ################################################
                # Mark firmware job SUCCESS
                ################################################

                cur.execute("""
                    UPDATE iot_device_firmware_updates
                    SET
                        update_status='SUCCESS',
                        completed_at=CURRENT_TIMESTAMP,
                        progress=100,
                        error_message=NULL
                    WHERE id=%s
                """, (
                    update_id,
                ))

                ################################################
                # Update actual device firmware version
                ################################################

                cur.execute("""
                    UPDATE iot_devices
                    SET firmware_version=%s
                    WHERE id=%s
                """, (
                    target_version,
                    device_id
                ))

                ################################################
                # Mark rollback history SUCCESS
                ################################################

                if update_type == "ROLLBACK":
                    cur.execute("""
                        UPDATE iot_firmware_rollbacks
                        SET
                            rollback_status='SUCCESS',
                            completed_at=CURRENT_TIMESTAMP,
                            error_message=NULL
                        WHERE rollback_update_id=%s
                    """, (
                        update_id,
                    ))

                conn.commit()

                emit_firmware_update(
                    device_id,
                    "SUCCESS",
                    100
                )

                print(
                    f"Firmware job {update_id} succeeded: "
                    f"device {device_id} now runs "
                    f"{target_version}"
                )

            else:

                error_message = (
                    "Simulated OTA update failure"
                )

                ################################################
                # Mark firmware job FAILED
                ################################################

                cur.execute("""
                    UPDATE iot_device_firmware_updates
                    SET
                        update_status='FAILED',
                        completed_at=CURRENT_TIMESTAMP,
                        progress=100,
                        error_message=%s
                    WHERE id=%s
                """, (
                    error_message,
                    update_id
                ))

                ################################################
                # Mark rollback history FAILED
                ################################################

                if update_type == "ROLLBACK":
                    cur.execute("""
                        UPDATE iot_firmware_rollbacks
                        SET
                            rollback_status='FAILED',
                            completed_at=CURRENT_TIMESTAMP,
                            error_message=%s
                        WHERE rollback_update_id=%s
                    """, (
                        error_message,
                        update_id
                    ))

                conn.commit()

                emit_firmware_update(
                    device_id,
                    "FAILED",
                    100
                )

                print(
                    f"Firmware job {update_id} failed: "
                    f"{error_message}"
                )

    except Exception as e:
        print(
            "Firmware update worker error:",
            e
        )

        if conn:
            conn.rollback()

    finally:
        if cur:
            cur.close()

        if conn:
            conn.close()

    time.sleep(2)