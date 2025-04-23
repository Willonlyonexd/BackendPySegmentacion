"""
Servidor API para RFM Segmentation
Versión simplificada con toda la funcionalidad en un archivo
"""

import os
import sys
import logging
from datetime import datetime
from flask import Flask, jsonify, request
from dotenv import load_dotenv

# Cargar nuestra funcionalidad RFM
import rfm_analysis

# Cargar variables de entorno
load_dotenv()

# Configuración
PORT = int(os.getenv('PORT', 5000))
DEBUG = os.getenv('DEBUG', 'True') == 'True'

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Crear aplicación Flask
app = Flask(__name__)

@app.route('/')
def home():
    """Página de inicio"""
    return """
    <html>
    <head><title>Segmentación RFM</title></head>
    <body>
        <h1>API de Segmentación RFM</h1>
        <p>Servicio activo.</p>
        <ul>
            <li><a href="/api/health">Verificar estado</a></li>
        </ul>
    </body>
    </html>
    """

@app.route('/api/health')
def health_check():
    """Endpoint para verificar la salud del servicio"""
    return jsonify({
        "status": "ok",
        "service": "rfm-segmentation",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/segmentation/run', methods=['POST'])
def trigger_segmentation():
    """Activar manualmente el proceso de segmentación RFM"""
    try:
        logger.info("Iniciando segmentación RFM desde API")
        result = rfm_analysis.run_segmentation()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error en segmentación: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/customer/segment/<customer_id>', methods=['GET'])
def get_customer_segment(customer_id):
    """Obtener segmento para un cliente específico"""
    try:
        segment = rfm_analysis.get_customer_segment(customer_id)
        if segment:
            return jsonify({
                "success": True,
                "data": segment
            })
        else:
            return jsonify({
                "success": False,
                "message": "Cliente no encontrado o no segmentado"
            }), 404
    except Exception as e:
        logger.error(f"Error obteniendo segmento: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/segmentation/status', methods=['GET'])
def get_segmentation_status():
    """Obtener el estado de la última segmentación"""
    try:
        db = rfm_analysis.get_db()
        
        # Obtener la última fecha de segmentación
        last_segment = db.customer_segments.find_one(sort=[("fecha_calculo", -1)])
        
        # Contar clientes por segmento
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
            "total_customers": sum(segment_counts.values()) if segment_counts else 0
        })
    except Exception as e:
        logger.error(f"Error obteniendo estado: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# Iniciar el servidor
if __name__ == "__main__":
    print("=== INICIANDO SERVIDOR RFM ===")
    print(f"Puerto: {PORT}")
    print(f"Debug: {DEBUG}")
    print(f"Servidor disponible en: http://localhost:{PORT}/")
    app.run(host='0.0.0.0', port=PORT, debug=DEBUG)