from pyModbusTCP.client import ModbusClient
from database import insert_data, get_active_device_configs
import time

while True:
    devices = get_active_device_configs()

    for d in devices:
        site = d[0]
        device_name = d[1]
        device_type = d[2]
        device_eui = d[3]
        protocol = d[4]
        host = d[5]
        port = d[6]
        start_register = d[7]
        register_count = d[8]

        if protocol != "MODBUS_TCP":
            continue

        client = ModbusClient(host=host, port=port, auto_open=True)

        regs = client.read_holding_registers(start_register, register_count)

        if regs:
            decoded = {
                "event": 1,
                "state": regs[3],
                "temperature": regs[0] / 10,
                "humidity": regs[1] / 10,
                "movement": "MODBUS",
                "battery": regs[2] / 100
            }

            insert_data(
                site,
                device_name,
                device_type,
                device_eui,
                "MODBUS_TCP",
                None,
                None,
                str(decoded),
                None
            )

            print(device_name, decoded)
        else:
            print("Read failed:", device_name)

        client.close()

    print("-" * 40)
    time.sleep(5)