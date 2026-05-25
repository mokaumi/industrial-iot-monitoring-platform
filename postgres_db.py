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
