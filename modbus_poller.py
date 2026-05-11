from pyModbusTCP.client import ModbusClient
from database import insert_data
import time

client = ModbusClient(host="127.0.0.1", port=5020, auto_open=True)

while True:
    regs = client.read_holding_registers(0, 4)

    if regs:
        temperature = regs[0] / 10
        humidity = regs[1] / 10
        battery = regs[2] / 100
        status = regs[3]

        decoded = {
            "event": 1,
            "state": status,
            "temperature": temperature,
            "humidity": humidity,
            "movement": "MODBUS",
            "battery": battery
        }

        insert_data(
            "MODBUS_SITE",
            "RS485 Temp Humidity Sensor",
            "temperature_sensor",
            "MODBUS_TEMP_001",
            "MODBUS_TCP",
            None,
            None,
            str(decoded),
            None
        )

        print("Saved Modbus reading:", decoded)

    else:
        print("Modbus read failed")

    time.sleep(5)
