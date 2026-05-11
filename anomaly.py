def analyze_temperature(temperature=None, humidity=None, battery=None, status="ONLINE", temp_history=None):
    score = 0
    reasons = []

    if temp_history is None:
        temp_history = []

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

    if len(temp_history) >= 5 and temperature is not None:
        previous_values = [t for t in temp_history[-5:-1] if t is not None]

        if previous_values:
            avg_temp = sum(previous_values) / len(previous_values)
            change = temperature - avg_temp

            if change >= 8:
                score += 30
                reasons.append(f"Sudden temperature rise (+{change:.1f}°C)")
            elif change <= -8:
                score += 20
                reasons.append(f"Sudden temperature drop ({change:.1f}°C)")

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

    if score >= 80:
        level = "HIGH"
    elif score >= 50:
        level = "MEDIUM"
    elif score >= 20:
        level = "LOW"
    else:
        level = "NORMAL"

    return {
        "anomaly_score": score,
        "anomaly_level": level,
        "anomaly_reasons": reasons
    }


def predict_temperature_trend(temp_history, minutes_ahead=15):
    values = [t for t in temp_history if t is not None]

    if len(values) < 3:
        return {
            "predicted_temperature": None,
            "prediction_risk": "UNKNOWN",
            "prediction_message": "Not enough data for prediction"
        }

    recent = values[-5:]

    first = recent[0]
    last = recent[-1]

    rate_per_reading = (last - first) / max(1, len(recent) - 1)

    predicted = last + (rate_per_reading * 3)

    if predicted >= 40:
        risk = "HIGH"
        message = "Temperature may reach unsafe level soon"
    elif predicted >= 35:
        risk = "MEDIUM"
        message = "Temperature is trending upward"
    else:
        risk = "LOW"
        message = "Temperature trend is stable"

    forecast_values = [
        round(last + (rate_per_reading * 1), 1),
        round(last + (rate_per_reading * 2), 1),
        round(last + (rate_per_reading * 3), 1)
    ]

    return {
        "predicted_temperature": round(predicted, 1),
        "prediction_risk": risk,
        "prediction_message": message,
        "forecast_values": forecast_values
    }