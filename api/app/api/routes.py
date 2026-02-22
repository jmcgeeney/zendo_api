from flask import jsonify

from app.api import api_bp

@api_bp.route("/energy_summary")
def summarize():
    return jsonify(
        {
            "production": 1000,
            "consumption": 800,
            "net": 200,
            "weather_summary": {
                "temperature": 25,
                "condition": "Sunny",
            },
            "correlation_metric": 0.85,
        }
    ), 200
