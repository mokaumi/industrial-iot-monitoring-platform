
import psycopg2



def check_gateway_offline_pg(timeout_minutes=2):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    UPDATE gateways
    SET status = 'OFFLINE'
    WHERE last_seen < NOW() - (%s || ' minutes')::interval
      AND status != 'OFFLINE'
      AND is_active = 1
    """, (timeout_minutes,))

    cur.execute("""
    INSERT INTO gateway_alarms (
        gateway_eui,
        parameter,
        severity,
        alarm_status,
        alarm_reason
    )
    SELECT
        gateway_eui,
        'gateway_status',
        'CRITICAL',
        'OPEN',
        'Gateway offline: no telemetry received'
    FROM gateways
    WHERE status = 'OFFLINE'
      AND is_active = 1
      AND NOT EXISTS (
          SELECT 1
          FROM gateway_alarms ga
          WHERE ga.gateway_eui = gateways.gateway_eui
            AND ga.parameter = 'gateway_status'
            AND ga.alarm_status IN ('OPEN','ACKNOWLEDGED')
      )
    """)

    conn.commit()
    cur.close()
    conn.close()



def calculate_gateway_status(cpu_usage, memory_usage, signal_quality, raw_status):
    if raw_status != "ONLINE":
        return "OFFLINE"

    if cpu_usage is None or memory_usage is None or signal_quality is None:
        return "UNKNOWN"

    if cpu_usage >= 90:
        return "DEGRADED"

    if memory_usage >= 90:
        return "DEGRADED"

    if signal_quality < 50:
        return "DEGRADED"

    return "ONLINE"


def clear_gateway_alarm_pg(gateway_eui, parameter):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT id
    FROM gateway_alarms
    WHERE gateway_eui = %s
        AND parameter = %s
        AND alarm_status IN ('OPEN', 'ACKNOWLEDGED')
    LIMIT 1
    """, (gateway_eui, parameter))

    alarm = cur.fetchone()

    if not alarm:
        cur.close()
        conn.close()
        return

    cur.execute("""
    UPDATE gateway_alarms
    SET alarm_status = 'CLEARED',
        cleared_at = CURRENT_TIMESTAMP
    WHERE id = %s
    """, (alarm[0],))

    conn.commit()

    log_gateway_event_pg(
        gateway_eui,
        "ALARM_CLEARED",
        f"{parameter} alarm cleared"
    )

    cur.close()
    conn.close()



def create_or_update_gateway_alarm_pg(
    gateway_eui,
    parameter,
    severity,
    reason
):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT id
    FROM gateway_alarms
    WHERE gateway_eui = %s
      AND parameter = %s
      AND alarm_status IN ('OPEN', 'ACKNOWLEDGED')
    """, (gateway_eui, parameter))

    row = cur.fetchone()

    if row:
        cur.execute("""
        UPDATE gateway_alarms
        SET last_seen = CURRENT_TIMESTAMP,
            alarm_reason = %s,
            severity = %s
        WHERE id = %s
        """, (reason, severity, row[0]))
    else:
        cur.execute("""
        INSERT INTO gateway_alarms (
            gateway_eui,
            parameter,
            severity,
            alarm_status,
            alarm_reason
        )
        VALUES (%s,%s,%s,'OPEN',%s)
        """, (gateway_eui, parameter, severity, reason))
        log_gateway_event_pg(
            gateway_eui,
            "ALARM_RAISED",
            reason
        )

    conn.commit()
    cur.close()
    conn.close()




def log_gateway_event_pg(
    gateway_eui,
    event_type,
    event_message
):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO gateway_events(
        gateway_eui,
        event_type,
        event_message
    )
    VALUES (%s,%s,%s)
    """, (
        gateway_eui,
        event_type,
        event_message
    ))

    conn.commit()

    cur.close()
    conn.close()





def insert_gateway_command_response_pg(
    gateway_eui,
    command,
    status,
    message,
    source
):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO gateway_command_responses (
        gateway_eui,
        command,
        status,
        message,
        source
    )
    VALUES (%s,%s,%s,%s,%s)
    """, (
        gateway_eui,
        command,
        status,
        message,
        source
    ))

    conn.commit()
    cur.close()
    conn.close()




def insert_gateway_telemetry_pg(
    gateway_eui,
    cpu_usage,
    memory_usage,
    signal_quality,
    packets_today,
    status
):

    calculated_status = calculate_gateway_status(
        cpu_usage,
        memory_usage,
        signal_quality,
        status
    )

    conn = get_pg_connection()
    cur = conn.cursor()

    # Save telemetry
    cur.execute("""
    INSERT INTO gateway_telemetry (
        gateway_eui,
        cpu_usage,
        memory_usage,
        signal_quality,
        packets_today,
        status
    )
    VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        gateway_eui,
        cpu_usage,
        memory_usage,
        signal_quality,
        packets_today,
        calculated_status
    ))

    # Update gateway master record
        # Insert telemetry
    cur.execute("""
    INSERT INTO gateway_telemetry (
        gateway_eui,
        cpu_usage,
        memory_usage,
        signal_quality,
        packets_today,
        status
    )
    VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        gateway_eui,
        cpu_usage,
        memory_usage,
        signal_quality,
        packets_today,
        calculated_status
    ))

    # Get previous gateway status
    cur.execute("""
    SELECT status
    FROM gateways
    WHERE gateway_eui=%s
    """, (gateway_eui,))

    old = cur.fetchone()
    old_status = old[0] if old else None

    # Update gateway record
    if old_status == "OFFLINE" and calculated_status != "OFFLINE":

        cur.execute("""
        UPDATE gateways
        SET status=%s,
            last_seen=CURRENT_TIMESTAMP,
            online_since=CURRENT_TIMESTAMP
        WHERE gateway_eui=%s
        """, (
            calculated_status,
            gateway_eui
        ))

    else:

        cur.execute("""
        UPDATE gateways
        SET status=%s,
            last_seen=CURRENT_TIMESTAMP
        WHERE gateway_eui=%s
        """, (
            calculated_status,
            gateway_eui
        ))
        
        
        
        
    if old_status and old_status != calculated_status:

        log_gateway_event_pg(
            gateway_eui,
            "STATUS_CHANGE",
            f"Gateway changed from "
            f"{old_status} to "
            f"{calculated_status}"
        )
    conn.commit()
    # --------------------------
    # Gateway Alarm Rules
    # --------------------------

    

    # CPU Alarm
    if cpu_usage >= 90:
        create_or_update_gateway_alarm_pg(
            gateway_eui,
            "cpu_usage",
            "CRITICAL",
            f"CPU usage above threshold: {cpu_usage}%"
        )
    else:
        clear_gateway_alarm_pg(
            gateway_eui,
            "cpu_usage"
        )

    # Memory Alarm
    if memory_usage >= 90:
        create_or_update_gateway_alarm_pg(
            gateway_eui,
            "memory_usage",
            "CRITICAL",
            f"Memory usage above threshold: {memory_usage}%"
        )
    else:
        clear_gateway_alarm_pg(
            gateway_eui,
            "memory_usage"
        )

    # Signal Quality Alarm
    if signal_quality < 50:
        create_or_update_gateway_alarm_pg(
            gateway_eui,
            "signal_quality",
            "WARNING",
            f"Signal quality below threshold: {signal_quality}%"
        )
    else:
        clear_gateway_alarm_pg(
            gateway_eui,
            "signal_quality"
        )

    # Gateway Offline Alarm
    if calculated_status == "OFFLINE":
        create_or_update_gateway_alarm_pg(
            gateway_eui,
            "gateway_status",
            "CRITICAL",
            f"Gateway status is {status}"
        )
    else:
        clear_gateway_alarm_pg(
            gateway_eui,
            "gateway_status"
        )

    cur.close()
    conn.close()





def update_reported_twin_pg(
    device_eui,
    reporting_interval,
    alarm_enabled
):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    UPDATE device_twin
    SET
        reported_reporting_interval = %s,
        reported_alarm_enabled = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE device_eui = %s
    """, (
        reporting_interval,
        alarm_enabled,
        device_eui
    ))

    conn.commit()
    cur.close()
    conn.close()

def update_desired_twin_pg(
    device_eui,
    reporting_interval,
    alarm_enabled
):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    UPDATE device_twin
    SET
        desired_reporting_interval = %s,
        desired_alarm_enabled = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE device_eui = %s
    """, (
        reporting_interval,
        alarm_enabled,
        device_eui
    ))

    conn.commit()
    cur.close()
    conn.close()





def get_device_twin_pg(device_eui):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT
        desired_reporting_interval,
        desired_alarm_enabled,
        reported_reporting_interval,
        reported_alarm_enabled
    FROM device_twin
    WHERE device_eui = %s
    """, (device_eui,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    return row




def create_device_twin_pg(device_eui):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO device_twin (device_eui)
    VALUES (%s)
    ON CONFLICT (device_eui) DO NOTHING
    """, (device_eui,))

    conn.commit()
    cur.close()
    conn.close()







def insert_command_response_pg(
    device_eui,
    command,
    status,
    message,
    source
):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO command_responses (
        device_eui,
        command,
        status,
        message,
        source
    )
    VALUES (%s, %s, %s, %s, %s)
    """, (
        device_eui,
        command,
        status,
        message,
        source
    ))

    conn.commit()
    cursor.close()
    conn.close()






def acknowledge_active_alarm_pg(alarm_id, acknowledged_by="admin"):
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE active_alarms
    SET alarm_status = 'ACKNOWLEDGED'
    WHERE id = %s
    AND alarm_status = 'OPEN'
    RETURNING id
    """, (alarm_id,))

    updated = cursor.fetchone()

    conn.commit()
    cursor.close()
    conn.close()

    return updated is not None





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
    AND alarm_status IN ('OPEN', 'ACKNOWLEDGED')
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
    AND alarm_status IN ('OPEN', 'ACKNOWLEDGED')
    """, (
        device_eui,
        parameter
    ))

    conn.commit()

    cursor.close()
    conn.close()




   
