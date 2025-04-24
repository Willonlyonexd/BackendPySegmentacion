# app.py

from flask import Flask, jsonify, request
from flask_cors import CORS
import sys  # ✅ ESTA LÍNEA ES NECESARIA
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import logging
from datetime import datetime
from dotenv import load_dotenv


from rfm_analysis import run_segmentation, get_customer_segment
from db.mongo import get_db

# --- Configuración general ---
load_dotenv()

PORT = int(os.environ.get("PORT", 5000))
DEBUG = os.environ.get("DEBUG", "False") == "True"

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger('app')

# --- Crear app Flask ---
app = Flask(__name__)
CORS(app)

# --- Endpoints ---

@app.route("/")
def home():
    return """
    <html>
    <head><title>Segmentación RFM</title></head>
    <body>
        <h1>API de Segmentación RFM</h1>
        <ul>
            <li><a href="/api/health">Verificar estado</a></li>
        </ul>
    </body>
    </html>
    """

@app.route("/api/health")
def health_check():
    return jsonify({
        "status": "ok",
        "service": "rfm-segmentation",
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/segmentation/run", methods=["POST"])
def trigger_segmentation():
    try:
        logger.info("Ejecutando segmentación desde API")
        result = run_segmentation()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error al ejecutar segmentación: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/customer/segment/<customer_id>", methods=["GET"])
def api_get_customer_segment(customer_id):
    try:
        segment = get_customer_segment(customer_id)
        if segment:
            return jsonify({"success": True, "data": segment})
        else:
            return jsonify({"success": False, "message": "Cliente no encontrado"}), 404
    except Exception as e:
        logger.error(f"Error obteniendo segmento: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/segmentation/status", methods=["GET"])
def get_segmentation_status():
    try:
        db = get_db()
        last_segment = db.customer_segments.find_one(sort=[("fecha_calculo", -1)])
        pipeline = [
            {"$group": {
                "_id": "$segmento",
                "count": {"$sum": 1}
            }}
        ]
        segment_counts = {}
        for doc in db.customer_segments.aggregate(pipeline):
            segment_counts[doc["_id"]] = doc["count"]

        return jsonify({
            "success": True,
            "last_update": last_segment["fecha_calculo"].isoformat() if last_segment else None,
            "segments": segment_counts,
            "total_customers": sum(segment_counts.values())
        })
    except Exception as e:
        logger.error(f"Error obteniendo estado de segmentación: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/segmentation/check-new-data", methods=["GET"])
def check_new_data():
    try:
        db = get_db()

        # Buscar la fecha del último segmento
        last_seg = db.customer_segments.find_one(sort=[("fecha_calculo", -1)])
        if not last_seg:
            return jsonify({"new_data_count": "unknown", "should_train": True})

        last_date = last_seg["fecha_calculo"]

        # Contar ventas nuevas desde entonces
        count = db.ventas.count_documents({
            "createdAT": {"$gt": last_date},
            "estado": {"$in": ["Procesado", "Completado", "Entregado"]}
        })

        return jsonify({
            "success": True,
            "new_data_count": count,
            "should_train": count > 50  # Puedes ajustar el umbral
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# --- Run App ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # ✅ Render asigna este puerto
    app.run(host="0.0.0.0", port=port)