from db.mongo import get_db
from datetime import datetime

def save_results_to_db(df):
    db = get_db()
    db.customer_segments.delete_many({})
    records = []
    for _, row in df.iterrows():
        records.append({
            "cliente_id": row["cliente_id"],
            "recencia_dias": float(row["Recencia"]),
            "num_compras": float(row["Frecuencia"]),
            "total_gastado": float(row["Monetario"]),
            "segmento": row["Segmento_Nombre"],
            "segmento_numero": int(row["Segmento"]),
            "fecha_calculo": datetime.now()
        })
    db.customer_segments.insert_many(records)
