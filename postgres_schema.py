import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="iot_platform",
    user="iot_user",
    password="iot_password",
    port=5432
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    site TEXT,
    device_name TEXT,
    device_type TEXT,
    device_eui TEXT,
    freq TEXT,
    rssi INTEGER,
    snr REAL,
    payload TEXT,
    temp REAL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    asset_id INTEGER,
    asset_name TEXT,
    asset_type TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS anomaly_events (
    id SERIAL PRIMARY KEY,
    site TEXT,
    device_eui TEXT,
    device_type TEXT,
    anomaly_score INTEGER,
    anomaly_level TEXT,
    anomaly_reason TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    incident_status TEXT DEFAULT 'OPEN',
    resolved_at TIMESTAMP
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS alarm_acknowledgements (
    id SERIAL PRIMARY KEY,
    device_eui TEXT,
    alarm_message TEXT,
    acknowledged_by TEXT,
    acknowledged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS asset_devices (
    id SERIAL PRIMARY KEY,
    asset_id INTEGER,
    device_eui TEXT,
    is_active INTEGER DEFAULT 1,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
conn.commit()
cursor.close()
conn.close()

print("PostgreSQL tables created successfully!")
