import time
import requests
from postgres_db import get_pg_connection


print("Firmware scheduler worker started...")


while True:
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            firmware_id,
            campaign_name
        FROM iot_firmware_schedules
        WHERE schedule_status='PENDING'
          AND scheduled_time <= CURRENT_TIMESTAMP
        ORDER BY scheduled_time ASC
        LIMIT 1
    """)

    schedule = cur.fetchone()

    if schedule:
        schedule_id = schedule[0]
        firmware_id = schedule[1]
        campaign_name = schedule[2]

        print("Executing scheduled firmware campaign:", schedule_id)

        try:
            res = requests.post(
                "http://flask_app:5000/create_firmware_campaign",
                json={
                    "firmware_id": firmware_id,
                    "campaign_name": campaign_name
                },
                timeout=5
            )

            if res.status_code == 200:
                cur.execute("""
                    UPDATE iot_firmware_schedules
                    SET schedule_status='EXECUTED',
                        executed_at=CURRENT_TIMESTAMP
                    WHERE id=%s
                """, (schedule_id,))
            else:
                cur.execute("""
                    UPDATE iot_firmware_schedules
                    SET schedule_status='FAILED',
                        executed_at=CURRENT_TIMESTAMP
                    WHERE id=%s
                """, (schedule_id,))

            conn.commit()

        except Exception as e:
            print("Scheduler error:", e)

    cur.close()
    conn.close()

    time.sleep(5)
