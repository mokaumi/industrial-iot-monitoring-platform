from pyModbusTCP.server import ModbusServer
import time
import math

server = ModbusServer(host="0.0.0.0", port=5020, no_block=True)
server.start()

print("Multi-device Modbus simulator running...")

counter = 0

try:
    while True:

        # Device 1
        temp1 = 25 + (5 * math.sin(counter / 5))
        hum1 = 60 + (10 * math.sin(counter / 8))

        # Device 2
        temp2 = 18 + (3 * math.sin(counter / 6))
        hum2 = 50 + (5 * math.sin(counter / 7))

        # Device 3
        temp3 = 75 + (7 * math.sin(counter / 4))
        hum3 = 40 + (8 * math.sin(counter / 5))

        # Registers for device 1
        server.data_bank.set_holding_registers(
            0,
            [int(temp1 * 10), int(hum1 * 10), 380, 1]
        )

        # Registers for device 2
        server.data_bank.set_holding_registers(
            10,
            [int(temp2 * 10), int(hum2 * 10), 370, 1]
        )

        # Registers for device 3
        server.data_bank.set_holding_registers(
            20,
            [int(temp3 * 10), int(hum3 * 10), 390, 1]
        )

        print(f"Cold Room Temp: {temp1:.1f}°C")
        print(f"Warehouse Temp: {temp2:.1f}°C")
        print(f"Generator Room Temp: {temp3:.1f}°C")
        print("-" * 40)

        counter += 1
        time.sleep(5)

except KeyboardInterrupt:
    server.stop()