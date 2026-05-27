import time
from app import create_offline_incidents

CHECK_INTERVAL = 30

print("Heartbeat worker started...")

while True:
    try:
        create_offline_incidents(timeout_seconds=60)
        print("Heartbeat check completed")
    except Exception as e:
        print("Heartbeat worker error:", e)

    time.sleep(CHECK_INTERVAL)
