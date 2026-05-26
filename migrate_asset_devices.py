import sqlite3
from postgres_db import get_pg_connection

sqlite_conn = sqlite3.connect("iot.db", timeout=10)
sqlite_cursor = sqlite_conn.cursor()

sqlite_cursor.execute("""
SELECT asset_id, device_eui, is_active, assigned_at
FROM asset_devices
""")

rows = sqlite_cursor.fetchall()

pg_conn = get_pg_connection()
pg_cursor = pg_conn.cursor()

for r in rows:
    pg_cursor.execute("""
    INSERT INTO asset_devices (
        asset_id,
        device_eui,
        is_active,
        assigned_at
    )
    VALUES (%s, %s, %s, %s)
    """, r)

pg_conn.commit()

sqlite_conn.close()
pg_cursor.close()
pg_conn.close()

print(f"Migrated {len(rows)} asset-device rows to PostgreSQL")
