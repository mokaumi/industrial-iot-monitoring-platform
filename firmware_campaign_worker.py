import time
import math
import requests
from postgres_db import get_pg_connection


def version_to_number(version):
    try:
        return int(version.replace("v", "").replace(".", ""))
    except:
        return 0


def emit_campaign_update(campaign_id):
    try:
        requests.post(
            "http://flask_app:5000/emit_firmware_campaign_update",
            json={"campaign_id": campaign_id},
            timeout=10
        )
    except Exception as e:
        print("Failed to emit campaign update:", e)


print("Firmware campaign worker started...")

while True:
    conn = None
    cur = None

    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        # Keep the rest of your campaign-worker logic here.

        cur.execute("""
            SELECT
                id,
                firmware_id,
                device_type,
                target_version,
                rollout_type,
                batch_size,
                rollout_percentage,
                canary_size,
                failure_threshold,
                canary_status
            FROM iot_firmware_campaigns
            WHERE campaign_status='PENDING'
            ORDER BY id ASC
            LIMIT 1
        """)

        campaign = cur.fetchone()
        print("Pending campaign:", campaign)

        if campaign:
            campaign_id = campaign[0]
            firmware_id = campaign[1]
            device_type = campaign[2]
            target_version = campaign[3]  
            rollout_type = campaign[4]
            batch_size = campaign[5]
            rollout_percentage = campaign[6]
            canary_size = campaign[7]
            failure_threshold = campaign[8]
            canary_status = campaign[9]

            print("Processing campaign:", campaign_id, target_version)

            cur.execute("""
                UPDATE iot_firmware_campaigns
                SET campaign_status='RUNNING'
                WHERE id=%s
            """, (campaign_id,))

            cur.execute("""
                SELECT id, firmware_version
                FROM iot_devices
                WHERE device_type=%s
            """, (device_type,))

            devices = cur.fetchall()

            eligible_devices = []

            for d in devices:
                device_id = d[0]
                current_version = d[1]

                if version_to_number(current_version) >= version_to_number(target_version):
                    continue

                eligible_devices.append(d)

            if rollout_type == "IMMEDIATE":

                selected_devices = eligible_devices


            elif rollout_type == "BATCH":

                safe_batch_size = batch_size or 1

                selected_devices = eligible_devices[:safe_batch_size]


            elif rollout_type == "PERCENTAGE":

                safe_percentage = rollout_percentage or 10

                percentage_wave_size = max(
                    1,
                    math.ceil(
                        len(eligible_devices)
                        * safe_percentage
                        / 100
                    )
                )

                selected_devices = eligible_devices[
                    :percentage_wave_size
                ]


            elif rollout_type == "CANARY":

                safe_canary_size = canary_size or 1

                selected_devices = eligible_devices[
                    :safe_canary_size
                ]

                cur.execute("""
                    UPDATE iot_firmware_campaigns
                    SET canary_status='CANARY_RUNNING'
                    WHERE id=%s
                """, (
                    campaign_id,
                ))

                print(
                    f"Campaign {campaign_id}: "
                    f"released CANARY group of "
                    f"{len(selected_devices)} device(s)"
                )


            else:

                print(
                    f"Campaign {campaign_id}: "
                    f"unknown rollout type {rollout_type}"
                )

                selected_devices = []

            jobs_created = 0

            for d in selected_devices:
                device_id = d[0]
                current_version = d[1]

                
                is_canary_job = (
                    rollout_type == "CANARY"
                    and canary_status == "NOT_STARTED"
                )

                cur.execute("""
                    INSERT INTO iot_device_firmware_updates
                    (
                        device_id,
                        current_version,
                        target_version,
                        update_status,
                        progress,
                        requested_by,
                        campaign_id,
                        is_canary
                    )
                    VALUES
                    (
                        %s,%s,%s,'PENDING',0,'Campaign',%s,%s
                    )
                """, (
                    device_id,
                    current_version,
                    target_version,
                    campaign_id,
                    is_canary_job
                ))

                jobs_created += 1

            final_status = "RUNNING" if jobs_created > 0 else "SUCCESS"

            cur.execute("""
                UPDATE iot_firmware_campaigns
                SET
                    total_devices=%s,
                    pending_count=%s,
                    running_count=0,
                    success_count=0,
                    failed_count=0,
                    campaign_status=%s,
                    completed_at = CASE
                        WHEN %s = 0 THEN CURRENT_TIMESTAMP
                        ELSE completed_at
                    END
                WHERE id=%s
            """, (
                len(eligible_devices),
                jobs_created,
                final_status,
                jobs_created,
                campaign_id
            ))
            
            
            
            cur.execute("""
            UPDATE iot_firmware_campaigns
            SET rollout_index=%s
            WHERE id=%s
            """,
            (
                len(selected_devices),
                campaign_id
            ))


            

            conn.commit()

            emit_campaign_update(campaign_id)

            print("Campaign jobs created:", jobs_created)

            
        # Recalculate statistics for all running campaigns
        cur.execute("""
            SELECT
                id,
                firmware_id,
                device_type,
                target_version,
                total_devices,
                rollout_type,
                batch_size,
                rollout_percentage,
                rollout_index,
                campaign_status,
                canary_size,
                failure_threshold,
                canary_status
            FROM iot_firmware_campaigns
            WHERE campaign_status IN ('RUNNING', 'PAUSED')
        """)
        running_campaigns = cur.fetchall()

        for rc in running_campaigns:
            campaign_id = rc[0]
            firmware_id = rc[1]
            device_type = rc[2]
            target_version = rc[3]
            total_devices = rc[4]

            rollout_type = rc[5]
            batch_size = rc[6]
            rollout_percentage = rc[7]
            rollout_index = rc[8]
            current_campaign_status = rc[9]
            
            canary_size = rc[10]
            failure_threshold = rc[11]
            canary_status = rc[12]

            cur.execute("""
                SELECT
                    update_status,
                    COUNT(*)
                FROM (
                    SELECT DISTINCT ON (device_id)
                        device_id,
                        update_status,
                        id
                    FROM iot_device_firmware_updates
                    WHERE campaign_id=%s
                    ORDER BY device_id, id DESC
                ) AS latest_attempts
                GROUP BY update_status
            """, (campaign_id,))

            counts = {
                "PENDING": 0,
                "RUNNING": 0,
                "SUCCESS": 0,
                "FAILED": 0
            }

            for row in cur.fetchall():
                counts[row[0]] = row[1]

            pending = counts["PENDING"]
            running = counts["RUNNING"]
            success = counts["SUCCESS"]
            failed = counts["FAILED"]
            
            
            
            
            ########################################################
            # Evaluate completed CANARY group
            ########################################################

            ########################################################
            # Evaluate CANARY group using CANARY jobs only
            ########################################################

            if (
                current_campaign_status == "RUNNING"
                and rollout_type == "CANARY"
                and canary_status in (
                    "CANARY_RUNNING",
                    "CANARY_RETRYING"
                )
            ):

                cur.execute("""
                    SELECT
                        update_status,
                        COUNT(*)
                    FROM (
                        SELECT DISTINCT ON (device_id)
                            device_id,
                            update_status,
                            id
                        FROM iot_device_firmware_updates
                        WHERE campaign_id=%s
                        AND is_canary=TRUE
                        ORDER BY device_id, id DESC
                    ) AS latest_canary_attempts
                    GROUP BY update_status
                """, (
                    campaign_id,
                ))

                canary_counts = {
                    "PENDING": 0,
                    "RUNNING": 0,
                    "SUCCESS": 0,
                    "FAILED": 0
                }

                for row in cur.fetchall():
                    canary_counts[row[0]] = row[1]

                canary_pending = canary_counts["PENDING"]
                canary_running = canary_counts["RUNNING"]
                canary_success = canary_counts["SUCCESS"]
                canary_failed = canary_counts["FAILED"]

                canary_completed = (
                    canary_success + canary_failed
                )

                print(
                    f"Campaign {campaign_id}: "
                    f"Canary status check - "
                    f"Pending={canary_pending}, "
                    f"Running={canary_running}, "
                    f"Success={canary_success}, "
                    f"Failed={canary_failed}"
                )

                ####################################################
                # Evaluate only when all Canary jobs have finished
                ####################################################

                if (
                    canary_pending == 0
                    and canary_running == 0
                    and canary_completed > 0
                ):

                    canary_failure_rate = (
                        canary_failed / canary_completed
                    ) * 100

                    print(
                        f"Campaign {campaign_id}: "
                        f"Canary completed. "
                        f"Success={canary_success}, "
                        f"Failed={canary_failed}, "
                        f"Failure rate={canary_failure_rate:.2f}%, "
                        f"Threshold={failure_threshold}%"
                    )

                    ################################################
                    # Canary FAILED
                    ################################################

                    if (
                        canary_failure_rate
                        > float(failure_threshold)
                    ):

                        cur.execute("""
                            UPDATE iot_firmware_campaigns
                            SET
                                campaign_status='PAUSED',
                                canary_status='CANARY_FAILED'
                            WHERE id=%s
                        """, (
                            campaign_id,
                        ))

                        current_campaign_status = "PAUSED"
                        canary_status = "CANARY_FAILED"

                        print(
                            f"Campaign {campaign_id}: "
                            f"CANARY FAILED. "
                            f"Campaign automatically paused."
                        )

                    ################################################
                    # Canary PASSED
                    ################################################

                    else:

                        cur.execute("""
                            UPDATE iot_firmware_campaigns
                            SET canary_status='CANARY_PASSED'
                            WHERE id=%s
                        """, (
                            campaign_id,
                        ))

                        canary_status = "CANARY_PASSED"

                        ################################################
                        # Find remaining eligible fleet devices
                        ################################################

                        cur.execute("""
                            SELECT
                                d.id,
                                d.firmware_version
                            FROM iot_devices d
                            WHERE d.device_type=%s
                            AND NOT EXISTS (
                                SELECT 1
                                FROM iot_device_firmware_updates u
                                WHERE u.campaign_id=%s
                                    AND u.device_id=d.id
                            )
                            ORDER BY d.id
                        """, (
                            device_type,
                            campaign_id
                        ))

                        remaining_devices = []

                        for d in cur.fetchall():

                            device_id = d[0]
                            current_version = d[1]

                            if (
                                version_to_number(current_version)
                                < version_to_number(target_version)
                            ):
                                remaining_devices.append(d)

                        ################################################
                        # Release remaining fleet as NON-CANARY jobs
                        ################################################

                        for d in remaining_devices:

                            cur.execute("""
                                INSERT INTO iot_device_firmware_updates
                                (
                                    device_id,
                                    current_version,
                                    target_version,
                                    update_status,
                                    progress,
                                    requested_by,
                                    campaign_id,
                                    is_canary
                                )
                                VALUES
                                (
                                    %s,
                                    %s,
                                    %s,
                                    'PENDING',
                                    0,
                                    'Campaign',
                                    %s,
                                    FALSE
                                )
                            """, (
                                d[0],
                                d[1],
                                target_version,
                                campaign_id
                            ))

                        if remaining_devices:

                            rollout_index += len(
                                remaining_devices
                            )

                            pending += len(
                                remaining_devices
                            )

                            cur.execute("""
                                UPDATE iot_firmware_campaigns
                                SET rollout_index=%s
                                WHERE id=%s
                            """, (
                                rollout_index,
                                campaign_id
                            ))

                        print(
                            f"Campaign {campaign_id}: "
                            f"CANARY PASSED. Released "
                            f"{len(remaining_devices)} "
                            f"remaining NON-CANARY device(s)."
                        )
            

            ########################################################
            # Release next staged rollout wave automatically
            ########################################################

            if (
                current_campaign_status == "RUNNING"
                and rollout_type in ("BATCH", "PERCENTAGE")
                and pending == 0
                and running == 0
            ):

                if rollout_type == "BATCH":
                    wave_size = batch_size or 1

                else:
                    safe_percentage = rollout_percentage or 10

                    wave_size = max(
                        1,
                        math.ceil(
                            total_devices * safe_percentage / 100
                        )
                    )

                # Find devices that do not already have a job
                # belonging to this campaign.
                cur.execute("""
                    SELECT
                        d.id,
                        d.firmware_version
                    FROM iot_devices d
                    WHERE d.device_type=%s
                    AND NOT EXISTS (
                        SELECT 1
                        FROM iot_device_firmware_updates u
                        WHERE u.campaign_id=%s
                            AND u.device_id=d.id
                    )
                    ORDER BY d.id
                """, (
                    device_type,
                    campaign_id
                ))

                remaining_devices = []

                for d in cur.fetchall():
                    device_id = d[0]
                    current_version = d[1]

                    if (
                        version_to_number(current_version)
                        < version_to_number(target_version)
                    ):
                        remaining_devices.append(d)

                next_wave = remaining_devices[:wave_size]

                for d in next_wave:
                    cur.execute("""
                        INSERT INTO iot_device_firmware_updates
                        (
                            device_id,
                            current_version,
                            target_version,
                            update_status,
                            progress,
                            requested_by,
                            campaign_id
                        )
                        VALUES
                        (%s,%s,%s,'PENDING',0,'Campaign',%s)
                    """, (
                        d[0],
                        d[1],
                        target_version,
                        campaign_id
                    ))

                if next_wave:
                    rollout_index += len(next_wave)
                    pending += len(next_wave)

                    cur.execute("""
                        UPDATE iot_firmware_campaigns
                        SET rollout_index=%s
                        WHERE id=%s
                    """, (
                        rollout_index,
                        campaign_id
                    ))

                    print(
                        f"Campaign {campaign_id}: "
                        f"released {rollout_type} wave "
                        f"of {len(next_wave)} device(s)"
                    )

            campaign_status = current_campaign_status

            if total_devices == 0:
                campaign_status = "SUCCESS"

            elif success + failed == total_devices:
                campaign_status = (
                    "SUCCESS"
                    if failed == 0
                    else "COMPLETED"
                )

            elif current_campaign_status == "PAUSED":
                campaign_status = "PAUSED"

            else:
                campaign_status = "RUNNING"
            cur.execute("""
                UPDATE iot_firmware_campaigns
                SET
                    pending_count=%s,
                    running_count=%s,
                    success_count=%s,
                    failed_count=%s,
                    campaign_status=%s,
                    completed_at = CASE
                        WHEN %s IN ('SUCCESS','COMPLETED')
                        THEN CURRENT_TIMESTAMP
                        ELSE completed_at
                    END
                WHERE id=%s
            """, (
                pending,
                running,
                success,
                failed,
                campaign_status,
                campaign_status,
                campaign_id
            ))

            emit_campaign_update(campaign_id)

        conn.commit()

    except Exception as e:

        print("Firmware campaign worker error:", e)

        if conn:
            conn.rollback()

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()

    time.sleep(5)
