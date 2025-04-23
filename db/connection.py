import pymongo
import certifi
import logging
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parámetros de conexión
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

def get_db_connection():
    """Establecer conexión a MongoDB y devolver cliente"""
    try:
        client = pymongo.MongoClient(
            MONGO_URI,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=5000
        )
        # Probar conexión
        client.server_info()
        logger.info("Conexión exitosa a MongoDB Atlas")
        return client
    except Exception as e:
        logger.error(f"Error de conexión a MongoDB: {str(e)}")
        raise

def get_db():
    """Obtener instancia de base de datos"""
    client = get_db_connection()
    return client[DB_NAME]

def test_connection():
    """Función de prueba para verificar la conexión"""
    try:
        db = get_db()
        # Listar colecciones para verificar conexión
        collections = db.list_collection_names()
        logger.info(f"Colecciones disponibles: {collections}")
        return True, collections
    except Exception as e:
        logger.error(f"Error probando conexión: {str(e)}")
        return False, str(e)