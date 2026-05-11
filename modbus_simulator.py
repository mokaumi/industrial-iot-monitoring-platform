from pyModbusTCP.server import ModbusServer
import time
import math

server = ModbusServer(host="0.0.0.0", port=5020, no_block=True)
server.start()

print("Modbus TCP simulator running on port 5020...")

counter = 0

try:
    while True:
        temperature = 25 + (5 * math.sin(counter / 5))
        humidity = 60 + (10 * math.sin(counter / 8))
        battery = 3.8
        status = 1

        registers = [
            int(temperature * 10),
            int(humidity * 10),
            int(battery * 100),
            status
        ]

        server.data_bank.set_holding_registers(0, registers)

        print(
            f"Temp: {temperature:.1f}°C | Humidity: {humidity:.1f}% | Battery: {battery}V"
        )

        counter += 1
        time.sleep(5)

except KeyboardInterrupt:
    server.stop()
    print("Modbus simulator stopped")