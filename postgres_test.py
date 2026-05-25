import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="iot_platform",
        user="iot_user",
        password="iot_password",
        port=5432
    )

    print("PostgreSQL connected successfully!")

    conn.close()

except Exception as e:
    print("PostgreSQL connection error:", e)
