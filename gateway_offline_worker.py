import time
from postgres_db import check_gateway_offline_pg

print("Gateway offline worker started...")

while True:
    check_gateway_offline_pg(timeout_minutes=2)
    print("Gateway offline check completed")
    time.sleep(30)
