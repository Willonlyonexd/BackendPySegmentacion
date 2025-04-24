from data.rfm_extractor import extract_rfm_data
from preprocessing.rfm_preprocessor import process_rfm_data
from clustering.rfm_cluster import train_kmeans_model
from models.model_persistence import save_results_to_db

def run_segmentation():
    print("=== INICIANDO ANÁLISIS RFM ===")
    rfm_data = extract_rfm_data()
    df_rfm_scaled = process_rfm_data(rfm_data)
    df_rfm_segments = train_kmeans_model(df_rfm_scaled)
    save_results_to_db(df_rfm_segments)
    return {
        "success": True,
        "segments": df_rfm_segments["Segmento_Nombre"].value_counts().to_dict(),
        "records_processed": len(rfm_data),
        "records_saved": len(df_rfm_segments)
    }

def get_customer_segment(customer_id):
    from db.mongo import get_db
    from bson import ObjectId
    db = get_db()
    segment = db.customer_segments.find_one({"cliente_id": customer_id}) or \
              (ObjectId.is_valid(customer_id) and db.customer_segments.find_one({"cliente_id": ObjectId(customer_id)}))
    if segment and "_id" in segment:
        segment["_id"] = str(segment["_id"])
    return segment