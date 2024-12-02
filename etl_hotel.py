import boto3
import pandas as pd
from sqlalchemy import create_engine
import os
import time
import mysql.connector
from mysql.connector import Error
from loguru import logger

# Configuración de Athena y MySQL
ATHENA_S3_OUTPUT = 's3://queries-results-hotel/Unsaved/2024/12/01/'  
REGION_NAME = 'us-east-1'

MYSQL_HOST = '44.223.120.162'
MYSQL_PORT = '8005'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'utec'
MYSQL_DB = 'prod'


# Configuración de logs
log_directory = "/app/logs"  # Uso de ruta absoluta para evitar confusión
os.makedirs(log_directory, exist_ok=True)
log_file = os.path.join(log_directory, "etl_hotel.log")
logger.add(log_file, format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name} | {message}", level="INFO", rotation="10 MB", retention="7 days")

# Ahora logger.info, logger.warning, logger.error deberían escribir en el archivo etl_hotel.log dentro de /app/logs



def create_database_if_not_exists():
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        cursor = connection.cursor()
        cursor.execute(f"SHOW DATABASES LIKE '{MYSQL_DB}'")
        result = cursor.fetchone()

        if not result:
            logger.info(f"La base de datos '{MYSQL_DB}' no existe. Creando la base de datos...")
            cursor.execute(f"CREATE DATABASE {MYSQL_DB}")
            logger.info(f"Base de datos '{MYSQL_DB}' creada correctamente.")
        else:
            logger.info(f"La base de datos '{MYSQL_DB}' ya existe.")

        cursor.close()
        connection.close()
    except Error as e:
        logger.error(f"Error al intentar conectar a MySQL o crear la base de datos: {e}")

def download_csv_from_s3(s3_bucket_path, local_path):
    try:
        s3 = boto3.client('s3', region_name=REGION_NAME)
        bucket_name = s3_bucket_path.split('/')[2]
        prefix = '/'.join(s3_bucket_path.split('/')[3:])
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', [])]

        if not files:
            logger.warning(f"No se encontraron archivos en la ruta S3: {s3_bucket_path}")
            return []

        csv_files = [file_key for file_key in files if file_key.endswith('.csv')]
        if not csv_files:
            logger.warning(f"No se encontraron archivos CSV en la ruta S3: {s3_bucket_path}")
            return []

        for file_key in csv_files:
            local_file_path = os.path.join(local_path, file_key.split('/')[-1])
            s3.download_file(bucket_name, file_key, local_file_path)
            logger.info(f"Archivo CSV descargado: {local_file_path}")

        return csv_files
    except Exception as e:
        logger.error(f"Error al descargar los archivos desde S3: {e}")
        return []

def insert_into_mysql(df, table_name):
    try:
        engine = create_engine(f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')
        df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=1000)
        logger.info(f"Datos insertados correctamente en la tabla {table_name} en MySQL.")
    except Exception as e:
        logger.error(f"Error al insertar datos en MySQL: {e}")

def main():
    logger.info("Inicio del proceso ETL")
    create_database_if_not_exists()

    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        if connection.is_connected():
            logger.info(f"Conexión exitosa a la base de datos '{MYSQL_DB}' en MySQL.")
        connection.close()
    except Error as e:
        logger.error(f"Error al conectar a la base de datos {MYSQL_DB}: {e}")
        return

    local_download_path = '/tmp/csv_files'
    os.makedirs(local_download_path, exist_ok=True)
    files = download_csv_from_s3(ATHENA_S3_OUTPUT, local_download_path)

    if not files:
        logger.warning("No se encontraron archivos CSV en el bucket.")
        return

    for file in files:
        local_file_path = os.path.join(local_download_path, file.split('/')[-1])
        try:
            df = pd.read_csv(local_file_path)
            logger.info(f"Archivo CSV {local_file_path} cargado exitosamente.")
            table_name = f'table_from_{file.split("/")[-1].split(".")[0]}'
            insert_into_mysql(df, table_name)
        except Exception as e:
            logger.error(f"Error al procesar el archivo {local_file_path}: {e}")

    logger.info("Fin del proceso ETL")

if __name__ == "__main__":
    main()
