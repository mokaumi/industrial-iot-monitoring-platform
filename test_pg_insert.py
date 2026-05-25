from postgres_db import insert_sensor_data_pg

insert_sensor_data_pg(
    site="Abia",
    device_name="Postgres Test Sensor",
    device_type="temperature_sensor",
    device_eui="PG_TEST_001",
    freq="MQTT",
    rssi=0,
    snr=0,
    payload="{'temperature': 55, 'humidity': 40, 'battery': 3.9}",
    temp=55,
    asset_id=1,
    asset_name="Generator",
    asset_type="AC METER"
)

print("PostgreSQL insert test completed")
