def analyze_temperature(temperature=None, humidity=None, battery=None, status="ONLINE"):
    score = 0
    reasons = []

    if status == "OFFLINE":
        score += 40
        reasons.append("Device offline")

    if temperature is not None:
        if temperature > 35:
            score += 35
            reasons.append("High temperature")
        elif temperature < 10:
            score += 25
            reasons.append("Low temperature")

    if humidity is not None and humidity > 85:
        score += 20
        reasons.append("High humidity")

    if battery is not None and battery < 3.0:
        score += 25
        reasons.append("Low battery")

    return build_result(score, reasons)


def analyze_smoke(fault=None, smoke=None, tamper=None, voltage=None, status="ONLINE"):
    score = 0
    reasons = []

    if status == "OFFLINE":
        score += 40
        reasons.append("Device offline")

    if smoke == 1:
        score += 60
        reasons.append("Smoke alarm")

    if fault == 1:
        score += 30
        reasons.append("Fault alarm")

    if tamper == 1:
        score += 25
        reasons.append("Tamper alarm")

    if voltage == 1:
        score += 20
        reasons.append("Voltage alarm")

    return build_result(score, reasons)


def analyze_ac_meter(voltage1=None, current1=None, frequency=None, pf1=None, status="ONLINE"):
    score = 0
    reasons = []

    if status == "OFFLINE":
        score += 40
        reasons.append("Device offline")

    if voltage1 is not None:
        if voltage1 > 250:
            score += 35
            reasons.append("Overvoltage")
        elif voltage1 < 200:
            score += 35
            reasons.append("Undervoltage")

    if current1 is not None and current1 > 20:
        score += 30
        reasons.append("Overcurrent")

    if frequency is not None and (frequency < 49 or frequency > 51):
        score += 25
        reasons.append("Abnormal frequency")

    if pf1 is not None and pf1 < 0.7:
        score += 20
        reasons.append("Low power factor")

    return build_result(score, reasons)


def build_result(score, reasons):
    score = min(score, 100)

    if score >= 70:
        level = "HIGH"
    elif score >= 35:
        level = "MEDIUM"
    elif score > 0:
        level = "LOW"
    else:
        level = "NORMAL"

    return {
        "anomaly_score": score,
        "anomaly_level": level,
        "anomaly_reasons": reasons
    }
