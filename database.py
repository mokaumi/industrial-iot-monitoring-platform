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

    conn.commit()
    conn.close()


def insert_data(site, device_name, device_type, device_eui, freq, rssi, snr, payload, temp):
    conn = sqlite3.connect("iot.db")
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO sensor_data (
        site, device_name, device_type, device_eui,
        freq, rssi, snr, payload, temp, timestamp
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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