from flask import jsonify, request, Blueprint
import logging
from bson import ObjectId
from db.connection import get_db
from core import run_rfm_analysis, get_customer_segment

logger = logging.getLogger(__name__)

rfm_blueprint = Blueprint('rfm', __name__, url_prefix='/api')

@rfm_blueprint.route('/health', methods=['GET'])
def health_check():
    """Verificar salud del servicio"""
    return jsonify({
        "status": "ok",
        "service": "rfm-segmentation"
    }), 200

@rfm_blueprint.route('/segmentation/run', methods=['POST'])
def trigger_segmentation():
    """Activar manualmente el proceso de segmentación RFM"""
    try:
        result = run_rfm_analysis()
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"Error al ejecutar la segmentación: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500

@rfm_blueprint.route('/customer/segment/<customer_id>', methods=['GET'])
def get_segment(customer_id):
    """Obtener segmento para un cliente específico"""
    try:
        segment = get_customer_segment(customer_id)
        if segment:
            return jsonify({"success": True, "data": segment}), 200
        else:
            return jsonify({"success": False, "message": "Cliente no encontrado"}), 404
    except Exception as e:
        logger.error(f"Error al obtener segmento del cliente: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500

@rfm_blueprint.route('/segmentation/status', methods=['GET'])
def get_status():
    """Obtener el estado de la última segmentación"""
    try:
        db = get_db()
        
        # Contar clientes por segmento
        pipeline = [
            {"$group": {
                "_id": "$segmento",
                "count": {"$sum": 1}
            }}
        ]
        
        segments_count = {}
        for doc in db.customer_segments.aggregate(pipeline):
            segments_count[doc["_id"]] = doc["count"]
            
        # Obtener la última fecha de cálculo
        last_calculation = db.customer_segments.find_one(sort=[("fecha_calculo", -1)])
        last_run_date = last_calculation["fecha_calculo"] if last_calculation else None
        
        return jsonify({
            "success": True,
            "last_run": last_run_date.isoformat() if last_run_date else None,
            "segments_count": segments_count,
            "total_customers": sum(segments_count.values()) if segments_count else 0
        }), 200
    except Exception as e:
        logger.error(f"Error al obtener estado de segmentación: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500