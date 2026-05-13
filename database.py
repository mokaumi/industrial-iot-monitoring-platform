import sqlite3
from datetime import datetime


def init_db():
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT,
        device_name TEXT,
        device_type TEXT,
        device_eui TEXT,
        freq TEXT,
        rssi INTEGER,
        snr REAL,
        payload TEXT,
        temp INTEGER,
        timestamp DATETIME
    )
    """)




    cursor.execute("""
    CREATE TABLE IF NOT EXISTS anomaly_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT,
        device_eui TEXT,
        device_type TEXT,
        anomaly_score INTEGER,
        anomaly_level TEXT,
        anomaly_reason TEXT,
        timestamp DATETIME
    )
    """)






    cursor.execute("""
    CREATE TABLE IF NOT EXISTS device_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT,
        device_name TEXT,
        device_type TEXT,
        device_eui TEXT UNIQUE,
        protocol TEXT,
        host TEXT,
        port INTEGER,
        start_register INTEGER,
        register_count INTEGER,
        polling_interval INTEGER,
        is_active INTEGER DEFAULT 1
    )
    """)



    cursor.execute("""
    CREATE TABLE IF NOT EXISTS assets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT,
        asset_name TEXT,
        asset_type TEXT,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)



    cursor.execute("""
    CREATE TABLE IF NOT EXISTS asset_devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset_id INTEGER,
        device_eui TEXT,
        is_active INTEGER DEFAULT 1,
        assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)




    conn.commit()
    conn.close()





def add_asset(site, asset_name, asset_type, description):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO assets (site, asset_name, asset_type, description)
    VALUES (?, ?, ?, ?)
    """, (site, asset_name, asset_type, description))

    conn.commit()
    conn.close()


def get_all_assets():
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT id, site, asset_name, asset_type, description, created_at
    FROM assets
    ORDER BY site, asset_name
    """)

    rows = cursor.fetchall()
    conn.close()

    return rows


def assign_device_to_asset(asset_id, device_eui):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE asset_devices
    SET is_active = 0
    WHERE asset_id = ?
    """, (asset_id,))

    cursor.execute("""
    INSERT INTO asset_devices (asset_id, device_eui, is_active)
    VALUES (?, ?, 1)
    """, (asset_id, device_eui))

    conn.commit()
    conn.close()





def insert_data(site, device_name, device_type, device_eui, freq, rssi, snr, payload, temp):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    asset = get_asset_by_device(device_eui)

    asset_id = None
    asset_name = None
    asset_type = None

    if asset:
        asset_id = asset[0]
        asset_name = asset[2]
        asset_type = asset[3]

    cursor.execute("""
    INSERT INTO sensor_data (
        site, device_name, device_type, device_eui,
        freq, rssi, snr, payload, temp, asset_id,
        asset_name, asset_type, timestamp
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        site,
        device_name,
        device_type,
        device_eui,
        freq,
        rssi,
        snr,
        payload,
        temp,
        asset_id,
        asset_name,
        asset_type,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))

    conn.commit()
    conn.close()


def get_all_data():
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM sensor_data ORDER BY id DESC LIMIT 50")
    rows = cursor.fetchall()

    conn.close()
    return rows


def get_data_by_device(site=None, device_eui=None):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    query = "SELECT * FROM sensor_data WHERE 1=1"
    params = []

    if site:
        query += " AND site = ?"
        params.append(site)

    if device_eui:
        query += " AND device_eui = ?"
        params.append(device_eui)

    query += " ORDER BY id DESC LIMIT 50"

    cursor.execute(query, params)
    rows = cursor.fetchall()

    conn.close()
    return rows


def get_devices():
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT DISTINCT site, device_name, device_type, device_eui
    FROM sensor_data
    ORDER BY site, device_name
    """)

    rows = cursor.fetchall()
    conn.close()

    return rows



def init_users_table():
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        role TEXT DEFAULT 'viewer'
    )
    """)

    conn.commit()
    conn.close()


def get_user_by_username(username):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE username=?", (username,))
    user = cursor.fetchone()

    conn.close()
    return user

# Admin user is created with username "admin" and password "admin123" (hashed)
def create_user(username, hashed_password, role):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO users(username,password,role) VALUES(?,?,?)",
        (username, hashed_password, role)
    )

    conn.commit()
    conn.close()


def get_all_users():
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("SELECT id, username, role FROM users")
    users = cursor.fetchall()

    conn.close()
    return users


def delete_user_by_id(user_id):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("SELECT username FROM users WHERE id=?", (user_id,))
    user = cursor.fetchone()

    if user and user[0] != "admin":
        cursor.execute("DELETE FROM users WHERE id=?", (user_id,))
        conn.commit()

    conn.close()



def insert_anomaly_event(site, device_eui, device_type, anomaly_score, anomaly_level, anomaly_reason):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO anomaly_events (
        site, device_eui, device_type,
        anomaly_score, anomaly_level, anomaly_reason, timestamp
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        site,
        device_eui,
        device_type,
        anomaly_score,
        anomaly_level,
        anomaly_reason,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))

    conn.commit()
    conn.close()


def recent_anomaly_exists(device_eui, anomaly_level, minutes=5):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT timestamp FROM anomaly_events
    WHERE device_eui = ?
    AND anomaly_level = ?
    ORDER BY id DESC
    LIMIT 1
    """, (device_eui, anomaly_level))

    row = cursor.fetchone()
    conn.close()

    if not row:
        return False

    last_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
    diff = (datetime.now() - last_time).total_seconds()

    return diff < minutes * 60


def get_recent_anomaly_events(limit=30):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT site, device_eui, device_type, anomaly_score,
           anomaly_level, anomaly_reason, timestamp
    FROM anomaly_events
    ORDER BY id DESC
    LIMIT ?
    """, (limit,))

    rows = cursor.fetchall()
    conn.close()

    return rows


def get_device_anomaly_stats(device_eui):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT COUNT(*), MAX(timestamp)
    FROM anomaly_events
    WHERE device_eui = ?
    """, (device_eui,))

    row = cursor.fetchone()

    conn.close()

    return {
        "total_anomalies": row[0] or 0,
        "last_anomaly": row[1]
    }


def add_device_config(site, device_name, device_type, device_eui, protocol,
                      host, port, start_register, register_count, polling_interval):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    INSERT OR IGNORE INTO device_config (
        site, device_name, device_type, device_eui,
        protocol, host, port, start_register,
        register_count, polling_interval
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        site, device_name, device_type, device_eui,
        protocol, host, port, start_register,
        register_count, polling_interval
    ))

    conn.commit()
    conn.close()


def get_active_device_configs():
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT site, device_name, device_type, device_eui,
           protocol, host, port, start_register,
           register_count, polling_interval
    FROM device_config
    WHERE is_active = 1
    """)

    rows = cursor.fetchall()
    conn.close()

    return rows



def get_all_device_configs():
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT id, site, device_name, device_type, device_eui,
           protocol, host, port, start_register,
           register_count, polling_interval, is_active
    FROM device_config
    ORDER BY site, device_name
    """)

    rows = cursor.fetchall()
    conn.close()

    return rows


def toggle_device_status(device_id):

    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE device_config
    SET is_active =
        CASE
            WHEN is_active = 1 THEN 0
            ELSE 1
        END
    WHERE id = ?
    """, (device_id,))

    conn.commit()
    conn.close()


def get_assets_with_devices():

    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        a.id,
        a.site,
        a.asset_name,
        a.asset_type,
        a.description,
        d.device_eui,
        d.assigned_at

    FROM assets a

    LEFT JOIN asset_devices d
        ON a.id = d.asset_id
        AND d.is_active = 1

    ORDER BY a.site, a.asset_name
    """)

    rows = cursor.fetchall()

    conn.close()

    return rows



def get_asset_by_device(device_eui):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        a.id,
        a.site,
        a.asset_name,
        a.asset_type,
        a.description
    FROM assets a
    JOIN asset_devices d
        ON a.id = d.asset_id
    WHERE d.device_eui = ?
    AND d.is_active = 1
    LIMIT 1
    """, (device_eui,))

    row = cursor.fetchone()
    conn.close()

    return row


def get_assets_by_site(site):

    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        id,
        asset_name,
        asset_type
    FROM assets
    WHERE site = ?
    ORDER BY asset_name
    """, (site,))

    rows = cursor.fetchall()

    conn.close()

    return rows



def get_data_by_asset(asset_id):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT *
    FROM sensor_data
    WHERE asset_id = ?
    ORDER BY timestamp DESC
    LIMIT 100
    """, (asset_id,))

    rows = cursor.fetchall()
    conn.close()

    return rows