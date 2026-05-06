# IoT Monitoring Platform

A full-stack industrial IoT monitoring and alerting platform built with Python Flask, SQLite, MQTT, UDP packet ingestion, and real-time web dashboards.

This system was developed to monitor multiple field devices such as:

- AC Energy Meters
- Temperature Sensors
- Smoke Detectors
- LoRaWAN Sensor Nodes

with live telemetry visualization, alert generation, user authentication, PDF reporting, and admin-level account management.

---

## Features

### Real-Time Device Monitoring
- Live Generator / Grid AC Meter dashboard
- Live Cold Room Temperature dashboard
- Live Smoke Detector dashboard
- Auto-refresh every 2 seconds
- Online / Offline device heartbeat monitoring

### Multi-Site / Multi-Device Support
- Dynamic site selector
- Dynamic device selector
- Automatic device discovery from database

### Intelligent Alerts
- Overvoltage / Undervoltage
- Overcurrent
- Low Power Factor
- Frequency abnormality
- Smoke / Fault / Tamper / Voltage alarms
- High temperature / low battery alerts
- Device offline notifications
- Toast pop-up alerts

### User Authentication & Security
- Login system
- Session-based route protection
- Password hashing with Werkzeug
- Role-based access:
  - Admin
  - Operator
  - Viewer

### Admin Control Center
- Create users
- Assign roles
- Delete users
- Protected admin routes

### Reporting
- PDF dashboard report export
- Tabular telemetry report export

### Backend Packet Ingestion
- MQTT subscriber listener
- UDP LoRa packet listener
- Automatic payload decoding

---

## Project Structure

```text
iot_monitoring_platform/
│
├── app.py
├── admin.py
├── auth.py
├── database.py
├── decoders.py
├── mqtt_handler.py
├── udp_handler.py
├── requirements.txt
├── iot.db
│
└── templates/
    ├── index.html
    ├── login.html
    └── users.html



Technology Stack

Python 3
Flask
SQLite3
Paho MQTT
HTML / CSS / JavaScript
Chart.js
jsPDF
html2canvas


Installation:


Clone project

git clone https://github.com/mokaumi/industrial-iot-monitoring-platform.git
cd industrial-iot-monitoring-platform


Create virtual environment

python3 -m venv lora-venv
source lora-venv/bin/activate


Install dependencies

pip install -r requirements.txt


Run application

python3 app.py


Default Admin Login

Username: admin
Password: admin123


Future Enhancements
Cloud deployment
REST API token authentication
AI anomaly detection
Email / SMS alert notifications
Customer-specific tenant dashboards
Author


Developed by Modu Kaumi
Industrial IoT / Energy Monitoring / Remote Telemetry Platform
