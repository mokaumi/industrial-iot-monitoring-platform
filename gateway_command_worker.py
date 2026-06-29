
import random
import time
from postgres_db import get_pg_connection, log_gateway_event_pg

print("Gateway command worker started...")

while True:
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            c.id,
            c.gateway_id,
            c.command_name,
            g.gateway_eui
        FROM gateway_commands c
        JOIN gateways g
            ON c.gateway_id = g.id
        WHERE c.command_status = 'PENDING'
        ORDER BY c.id ASC
        LIMIT 1
    """)

    command = cur.fetchone()

    if command:
        command_id = command[0]
        gateway_id = command[1]
        command_name = command[2]
        gateway_eui = command[3]

        print("Processing command:", command_id, command_name)

        cur.execute("""
            UPDATE gateway_commands
            SET command_status = 'RUNNING'
            WHERE id = %s
        """, (command_id,))

        conn.commit()

        time.sleep(10)

        cur.execute("""
            UPDATE gateway_commands
            SET command_status = 'SUCCESS',
                completed_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (command_id,))

        conn.commit()
        
        
        log_gateway_event_pg(
            gateway_eui,
            "COMMAND_STARTED",
            f"{command_name} started"
        )
        

        success = random.choice([True, True, True, False])

        if success:
            cur.execute("""
                UPDATE gateway_commands
                SET command_status = 'SUCCESS',
                    completed_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (command_id,))

            conn.commit()

            log_gateway_event_pg(
                gateway_eui,
                "COMMAND_COMPLETED",
                f"{command_name} completed successfully"
            )

            print("Command completed:", command_id)

        else:
            cur.execute("""
                UPDATE gateway_commands
                SET command_status = 'FAILED',
                    completed_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (command_id,))

            conn.commit()

            log_gateway_event_pg(
                gateway_eui,
                "COMMAND_FAILED",
                f"{command_name} failed"
            )

            print("Command failed:", command_id)
    cur.close()
    conn.close()

    time.sleep(2)
