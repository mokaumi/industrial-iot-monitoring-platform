
import psycopg2



def active_alarm_exists_pg(device_eui, parameter):

    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT id
    FROM active_alarms
    WHERE device_eui = %s
    AND parameter = %s
    AND alarm_status = 'OPEN'
    """, (
        device_eui,
        parameter
    ))

    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return result is not None




def get_pg_connection():
    return psycopg2.connect(
        host="postgres",
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




def get_alarm_rules_for_device_pg(device_eui):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT parameter, min_value, max_value, clear_margin, severity
    FROM alarm_rules
    WHERE device_eui = %s
    AND is_active = 1
    """, (device_eui,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows


def evaluate_alarm_rules_pg(device_eui, telemetry):

    rules = get_alarm_rules_for_device_pg(device_eui)

    alarms = []

    for rule in rules:

        parameter = rule[0]
        min_value = rule[1]
        max_value = rule[2]
        clear_margin = rule[3]
        severity = rule[4]

        if parameter not in telemetry:
            continue

        try:
            value = float(telemetry[parameter])
        except:
            continue

        alarm_open = active_alarm_exists_pg(
            device_eui,
            parameter
        )

        #
        # HIGH ALARM
        #
        if max_value is not None:

            if not alarm_open:

                if value > max_value:

                    alarms.append({
                        "parameter": parameter,
                        "value": value,
                        "severity": severity,
                        "reason":
                            f"{parameter} above maximum: "
                            f"{value} > {max_value}"
                    })

            else:

                # Keep alarm active until below hysteresis level

                if value > (max_value - clear_margin):

                    alarms.append({
                        "parameter": parameter,
                        "value": value,
                        "severity": severity,
                        "reason":
                            f"{parameter} still above hysteresis level"
                    })

        #
        # LOW ALARM
        #
        if min_value is not None:

            if not alarm_open:

                if value < min_value:

                    alarms.append({
                        "parameter": parameter,
                        "value": value,
                        "severity": severity,
                        "reason":
                            f"{parameter} below minimum: "
                            f"{value} < {min_value}"
                    })

            else:

                if value < (min_value + clear_margin):

                    alarms.append({
                        "parameter": parameter,
                        "value": value,
                        "severity": severity,
                        "reason":
                            f"{parameter} still below hysteresis level"
                    })

    return alarms



def create_or_update_active_alarm_pg(
    device_eui,
    parameter,
    alarm_reason,
    severity
):

    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT id
    FROM active_alarms
    WHERE device_eui = %s
    AND parameter = %s
    AND alarm_status = 'OPEN'
    """, (device_eui, parameter))

    existing = cursor.fetchone()

    if existing:

        cursor.execute("""
        UPDATE active_alarms
        SET last_seen = CURRENT_TIMESTAMP
        WHERE id = %s
        """, (existing[0],))

    else:

        cursor.execute("""
        INSERT INTO active_alarms
        (
            device_eui,
            parameter,
            alarm_reason,
            severity
        )
        VALUES (%s,%s,%s,%s)
        """, (
            device_eui,
            parameter,
            alarm_reason,
            severity
        ))
        
        conn.commit()
        cursor.close()
        conn.close()

    



def clear_active_alarm_pg(
    device_eui,
    parameter
):

    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE active_alarms
    SET
        alarm_status = 'CLEARED',
        cleared_at = CURRENT_TIMESTAMP
    WHERE device_eui = %s
    AND parameter = %s
    AND alarm_status = 'OPEN'
    """, (
        device_eui,
        parameter
    ))

    conn.commit()

    cursor.close()
    conn.close()




   
