from postgres_db import get_pg_connection

conn = get_pg_connection()
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


cursor.execute("""
CREATE TABLE IF NOT EXISTS devices (
    id SERIAL PRIMARY KEY,
    device_eui TEXT UNIQUE NOT NULL,
    device_name TEXT NOT NULL,
    device_type TEXT NOT NULL,
    site TEXT,
    asset_id INTEGER,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")



cursor.execute("""
CREATE TABLE IF NOT EXISTS alarm_rules (
    id SERIAL PRIMARY KEY,
    device_eui TEXT NOT NULL,
    parameter TEXT NOT NULL,
    min_value REAL,
    max_value REAL,
    clear_margin REAL DEFAULT 0,
    severity TEXT DEFAULT 'MEDIUM',
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")




cursor.execute("""
CREATE TABLE IF NOT EXISTS active_alarms (
    id SERIAL PRIMARY KEY,
    device_eui TEXT NOT NULL,
    parameter TEXT NOT NULL,
    alarm_reason TEXT NOT NULL,
    severity TEXT NOT NULL,
    alarm_status TEXT DEFAULT 'OPEN',
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cleared_at TIMESTAMP
);
""")


cursor.execute("""
CREATE TABLE IF NOT EXISTS command_responses (
    id SERIAL PRIMARY KEY,
    device_eui TEXT,
    command TEXT,
    status TEXT,
    message TEXT,
    source TEXT,
    response_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")



cursor.execute("""
CREATE TABLE IF NOT EXISTS device_twin (
    id SERIAL PRIMARY KEY,
    device_eui TEXT UNIQUE,

    desired_reporting_interval INTEGER DEFAULT 60,
    desired_alarm_enabled BOOLEAN DEFAULT TRUE,

    reported_reporting_interval INTEGER DEFAULT 60,
    reported_alarm_enabled BOOLEAN DEFAULT TRUE,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

conn.commit()
cursor.close()
conn.close()

print("PostgreSQL tables created successfully!")
