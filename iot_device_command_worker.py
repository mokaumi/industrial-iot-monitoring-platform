import time
import random
from postgres_db import get_pg_connection

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

        cur.execute("""
            UPDATE iot_device_commands
            SET command_status='RUNNING'
            WHERE id=%s
        """, (command_id,))
        conn.commit()

        time.sleep(5)

        success = random.choice([True, True, True, False])

        if success:
            cur.execute("""
                UPDATE iot_device_commands
                SET command_status='SUCCESS',
                    completed_at=CURRENT_TIMESTAMP
                WHERE id=%s
            """, (command_id,))
        else:
            cur.execute("""
                UPDATE iot_device_commands
                SET command_status='FAILED',
                    completed_at=CURRENT_TIMESTAMP
                WHERE id=%s
            """, (command_id,))

        conn.commit()

    cur.close()
    conn.close()

    time.sleep(2)
