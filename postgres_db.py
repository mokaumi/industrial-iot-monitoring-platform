import psycopg2


def get_pg_connection():
    return psycopg2.connect(
        host="localhost",
        database="iot_platform",
        user="iot_user",
        password="iot_password",
        port=5432
    )
def insert_sensor_data_pg(
    site,
    device_name,
    device_type,
    device_eui,
    freq,
    rssi,
    snr,
    payload,
    temp,
    asset_id=None,
    asset_name=None,
    asset_type=None
):

    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO sensor_data (
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
        asset_type
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        asset_type
    ))

    conn.commit()
    cursor.close()
    conn.close()


def get_data_by_asset_pg(asset_id):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        id,
        site,
        device_name,
        device_type,
        device_eui,
        freq,
        rssi,
        snr,
        payload,
        temp,
        timestamp,
        asset_id,
        asset_name,
        asset_type
    FROM sensor_data
    WHERE asset_id = %s
    ORDER BY timestamp DESC
    LIMIT 100
    """, (asset_id,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows



def insert_anomaly_event_pg(
    site,
    device_eui,
    device_type,
    anomaly_score,
    anomaly_level,
    anomaly_reason,
    incident_status="OPEN"
):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO anomaly_events (
        site,
        device_eui,
        device_type,
        anomaly_score,
        anomaly_level,
        anomaly_reason,
        incident_status
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        site,
        device_eui,
        device_type,
        anomaly_score,
        anomaly_level,
        anomaly_reason,
        incident_status
    ))

    conn.commit()
    cursor.close()
    conn.close()
    
    
def recent_anomaly_exists_pg(device_eui, anomaly_level, minutes=5):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT COUNT(*)
    FROM anomaly_events
    WHERE device_eui = %s
    AND anomaly_level = %s
    AND timestamp >= NOW() - (%s || ' minutes')::interval
    """, (
        device_eui,
        anomaly_level,
        minutes
    ))

    count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return count > 0



def resolve_open_incidents_pg(device_eui):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE anomaly_events
    SET incident_status = 'RESOLVED',
        resolved_at = NOW()
    WHERE device_eui = %s
    AND incident_status IN ('OPEN', 'ACKNOWLEDGED')
    AND anomaly_reason ILIKE 'Device offline%%'
    """, (device_eui,))

    conn.commit()
    cursor.close()
    conn.close()