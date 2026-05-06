import socket
import json
import base64

from database import insert_data
from decoders import decode_payload


def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 1700))

    while True:
        data, addr = sock.recvfrom(4096)

        try:
            payload = data[12:]
            msg = json.loads(payload)

            if "rxpk" in msg:
                for p in msg["rxpk"]:
                    raw_bytes = base64.b64decode(p["data"])
                    hex_payload = raw_bytes.hex()

                    print("UDP HEX:", hex_payload)

                    decoded = decode_payload(hex_payload)

                    print("UDP Decoded:", decoded)

                    insert_data(
                        "UNKNOWN_SITE",
                        "UDP_DEVICE",
                        "udp_lora_device",
                        "UNKNOWN_EUI",
                        p.get("freq"),
                        p.get("rssi"),
                        p.get("lsnr"),
                        str(decoded),
                        decoded.get("temp") if isinstance(decoded, dict) else None
                    )

        except Exception as e:
            print("UDP Decode error:", e)