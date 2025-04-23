"""
RFM Analysis - Análisis y segmentación de clientes
Versión simplificada con toda la funcionalidad en un archivo
"""

import os
import sys
import logging
import pymongo
import certifi
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from bson import ObjectId
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://houwenvt:will@cluster0.crz8eun.mongodb.net/EcommerML')
DB_NAME = os.getenv('DB_NAME', 'EcommerML')
NUM_CLUSTERS = int(os.getenv('NUM_CLUSTERS', 4))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_db():
    """Obtener conexión a la base de datos"""
    try:
        client = pymongo.MongoClient(
            MONGO_URI,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=5000
        )
        # Probar conexión
        client.server_info()
        logger.info("Conexión exitosa a MongoDB Atlas")
        return client[DB_NAME]
    except Exception as e:
        logger.error(f"Error de conexión a MongoDB: {str(e)}")
        raise

def extract_rfm_data():
    """Extraer datos RFM desde MongoDB"""
    db = get_db()
    
    # Fecha actual para cálculo de recencia
    fecha_actual = datetime.now()
    logger.info(f"Fecha de referencia para recencia: {fecha_actual}")
    
    pipeline = [
        # Filtrar ventas completadas
        {"$match": {"estado": {"$in": ["Procesado", "Completado", "Entregado"]}}},
        
        # Agrupar por cliente
        {"$group": {
            "_id": "$cliente",
            "ultima_compra": {"$max": "$createdAT"},
            "num_compras": {"$sum": 1},
            "total_gastado": {"$sum": "$total"}
        }},
        
        # Calcular recencia en días
        {"$project": {
            "cliente_id": "$_id",
            "recencia_dias": {
                "$dateDiff": {
                    "startDate": "$ultima_compra",
                    "endDate": fecha_actual,
                    "unit": "day"
                }
            },
            "num_compras": 1,
            "total_gastado": 1
        }}
    ]
    
    try:
        rfm_data = list(db.ventas.aggregate(pipeline, allowDiskUse=True))
        logger.info(f"Datos RFM extraídos: {len(rfm_data)} registros")
        
        # Convertir ObjectId a string para facilitar procesamiento
        for item in rfm_data:
            if isinstance(item["cliente_id"], ObjectId):
                item["cliente_id"] = str(item["cliente_id"])
                
        return rfm_data
    except Exception as e:
        logger.error(f"Error ejecutando pipeline de agregación: {str(e)}")
        raise

def process_rfm_data(rfm_data):
    """Procesar y escalar datos RFM"""
    try:
        # Convertir a DataFrame
        df_rfm = pd.DataFrame(rfm_data)
        
        # Verificar si hay datos suficientes
        if len(df_rfm) == 0:
            logger.warning("No hay datos RFM para procesar")
            return pd.DataFrame()
            
        # Verificar columnas necesarias
        required_cols = ["cliente_id", "recencia_dias", "num_compras", "total_gastado"]
        if not all(col in df_rfm.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df_rfm.columns]
            logger.error(f"Faltan columnas requeridas: {missing}")
            raise ValueError(f"Faltan columnas requeridas: {missing}")
        
        # Renombrar columnas
        df_rfm.rename(columns={
            "recencia_dias": "Recencia",
            "num_compras": "Frecuencia",
            "total_gastado": "Monetario"
        }, inplace=True)
        
        # Convertir a tipo numérico y manejar posibles errores
        for col in ["Recencia", "Frecuencia", "Monetario"]:
            df_rfm[col] = pd.to_numeric(df_rfm[col], errors='coerce')
            
        # Eliminar nulos y duplicados
        before_clean = len(df_rfm)
        df_rfm = df_rfm.dropna(subset=["Recencia", "Frecuencia", "Monetario"]).drop_duplicates()
        after_clean = len(df_rfm)
        
        if before_clean > after_clean:
            logger.warning(f"Se eliminaron {before_clean - after_clean} filas con valores nulos o duplicados")
        
        # Inversión de Recencia (menor recencia = mejor)
        max_recencia = df_rfm["Recencia"].max()
        df_rfm["Recencia"] = max_recencia - df_rfm["Recencia"]
        
        # Seleccionar métricas RFM
        rfm_metrics = df_rfm[["Recencia", "Frecuencia", "Monetario"]]
        
        # Escalar datos
        scaler = StandardScaler()
        rfm_scaled = scaler.fit_transform(rfm_metrics)
        
        # Crear DataFrame con datos escalados
        df_rfm_scaled = pd.DataFrame(rfm_scaled, columns=["Recencia", "Frecuencia", "Monetario"])
        df_rfm_scaled["cliente_id"] = df_rfm["cliente_id"].values
        
        logger.info("Datos RFM procesados y escalados exitosamente")
        return df_rfm_scaled
    except Exception as e:
        logger.error(f"Error procesando datos RFM: {str(e)}")
        raise

def train_kmeans_model(df_rfm_scaled):
    """Entrenar modelo KMeans en datos RFM"""
    try:
        if len(df_rfm_scaled) == 0:
            logger.warning("No hay datos suficientes para entrenar el modelo")
            return df_rfm_scaled
            
        # Seleccionar características para clustering
        rfm_features = df_rfm_scaled[["Recencia", "Frecuencia", "Monetario"]]
        
        # Entrenar modelo KMeans
        kmeans = KMeans(n_clusters=NUM_CLUSTERS, random_state=42, n_init=10)
        df_rfm_scaled["Segmento"] = kmeans.fit_predict(rfm_features)
        
        # Calcular centroides para interpretación
        centroids = kmeans.cluster_centers_
        
        # Analizar centroides para asignar nombres adecuadamente
        centroid_sums = centroids.sum(axis=1)
        centroid_ranks = np.argsort(-centroid_sums)  # Ordenar de mayor a menor valor
        
        # Definir nombres de segmentos según centroides
        segment_interpretation = {}
        
        # VIP: mayor valor combinado de RFM
        segment_interpretation[centroid_ranks[0]] = "VIP"
        
        # Dormidos: menor valor combinado de RFM
        segment_interpretation[centroid_ranks[-1]] = "Dormidos"
        
        # Fieles: segundo mejor valor combinado
        segment_interpretation[centroid_ranks[1]] = "Fieles"
        
        # Ocasionales: tercer mejor valor combinado
        segment_interpretation[centroid_ranks[2]] = "Ocasionales"
        
        # Añadir nombres de segmentos
        df_rfm_scaled["Segmento_Nombre"] = df_rfm_scaled["Segmento"].map(segment_interpretation)
        
        logger.info(f"Modelo KMeans entrenado con {NUM_CLUSTERS} clusters")
        logger.info(f"Interpretación de segmentos: {segment_interpretation}")
        
        # Contar clientes por segmento
        segment_counts = df_rfm_scaled["Segmento_Nombre"].value_counts().to_dict()
        logger.info(f"Distribución de clientes por segmento: {segment_counts}")
        
        return df_rfm_scaled
    except Exception as e:
        logger.error(f"Error entrenando modelo KMeans: {str(e)}")
        raise

def save_results_to_db(df_rfm_segments):
    """Guardar resultados de segmentación en MongoDB"""
    db = get_db()
    
    # Transformar datos para guardar
    rfm_transformed = []
    for _, row in df_rfm_segments.iterrows():
        rfm_transformed.append({
            "cliente_id": row["cliente_id"],
            "recencia_dias": float(row["Recencia"]) if isinstance(row["Recencia"], (int, float)) else 0,
            "num_compras": float(row["Frecuencia"]) if isinstance(row["Frecuencia"], (int, float)) else 0,
            "total_gastado": float(row["Monetario"]) if isinstance(row["Monetario"], (int, float)) else 0,
            "segmento": row["Segmento_Nombre"],
            "segmento_numero": int(row["Segmento"]) if isinstance(row["Segmento"], (int, float)) else 0,
            "fecha_calculo": datetime.now()
        })
    
    try:
        # Crear colección si no existe
        if "customer_segments" not in db.list_collection_names():
            db.create_collection("customer_segments")
            db.customer_segments.create_index("cliente_id", unique=True)
            
        # Borrar resultados anteriores
        if rfm_transformed:
            db.customer_segments.delete_many({})
            
            # Insertar nuevos resultados
            db.customer_segments.insert_many(rfm_transformed)
        
        logger.info(f"Guardados {len(rfm_transformed)} segmentos de clientes en la base de datos")
        return len(rfm_transformed)
    except Exception as e:
        logger.error(f"Error guardando resultados en la base de datos: {str(e)}")
        raise

def get_customer_segment(customer_id):
    """Obtener segmento para un cliente específico"""
    db = get_db()
    
    try:
        # Intentar con string
        result = db.customer_segments.find_one({"cliente_id": customer_id})
        if not result and ObjectId.is_valid(customer_id):
            # Intentar con ObjectId
            result = db.customer_segments.find_one({"cliente_id": ObjectId(customer_id)})
            
        if result:
            # Convertir ObjectId a string para serialización JSON
            if "_id" in result:
                result["_id"] = str(result["_id"])
            return result
        return None
    except Exception as e:
        logger.error(f"Error obteniendo segmento de cliente: {str(e)}")
        raise

def run_segmentation():
    """Función principal para ejecutar el proceso completo de segmentación"""
    try:
        logger.info("=== INICIANDO ANÁLISIS RFM ===")
        
        # 1. Extraer datos
        logger.info("Extrayendo datos RFM...")
        rfm_data = extract_rfm_data()
        logger.info(f"Datos extraídos: {len(rfm_data)} clientes")
        
        # 2. Procesar datos
        logger.info("Procesando y escalando datos...")
        df_rfm_scaled = process_rfm_data(rfm_data)
        
        # 3. Segmentar clientes con K-means
        logger.info("Entrenando modelo K-means...")
        df_rfm_segments = train_kmeans_model(df_rfm_scaled)
        
        # 4. Guardar resultados
        logger.info("Guardando resultados en la base de datos...")
        records_saved = save_results_to_db(df_rfm_segments)
        
        logger.info(f"=== ANÁLISIS COMPLETADO: {records_saved} registros guardados ===")
        return {
            "success": True,
            "segments": df_rfm_segments["Segmento_Nombre"].value_counts().to_dict() if not df_rfm_segments.empty else {},
            "records_processed": len(rfm_data),
            "records_saved": records_saved
        }
    except Exception as e:
        logger.error(f"ERROR EN LA SEGMENTACIÓN: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

# Ejecución directa para pruebas
if __name__ == "__main__":
    print("=== EJECUTANDO ANÁLISIS RFM ===")
    result = run_segmentation()
    print(f"Resultado: {result}")
    print("=== FINALIZADO ===")
    input("Presiona Enter para salir...")