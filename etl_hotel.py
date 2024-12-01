import boto3
import pandas as pd
from sqlalchemy import create_engine
import os
import time
import mysql.connector
from mysql.connector import Error

# Configuración de Athena y MySQL
ATHENA_S3_OUTPUT = 's3://queries-results-hotel/Unsaved/2024/12/01/'  
REGION_NAME = 'us-east-1'

MYSQL_HOST = '44.220.154.70'
MYSQL_PORT = '8005'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'utec'
MYSQL_DB = 'prod'

# Función para verificar si la base de datos existe, y crearla si no existe
def create_database_if_not_exists():
    try:
        # Conectar a MySQL sin especificar base de datos para poder crearla
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )

        cursor = connection.cursor()
        # Comprobar si la base de datos ya existe
        cursor.execute(f"SHOW DATABASES LIKE '{MYSQL_DB}'")
        result = cursor.fetchone()

        if not result:
            print(f"La base de datos '{MYSQL_DB}' no existe. Creando la base de datos...")
            cursor.execute(f"CREATE DATABASE {MYSQL_DB}")
            print(f"Base de datos '{MYSQL_DB}' creada correctamente.")
        else:
            print(f"La base de datos '{MYSQL_DB}' ya existe.")

        # Cerrar la conexión
        cursor.close()
        connection.close()

    except Error as e:
        print(f"Error al intentar conectar a MySQL o crear la base de datos: {e}")

# Función para descargar solo archivos CSV desde S3
def download_csv_from_s3(s3_bucket_path, local_path):
    try:
        s3 = boto3.client('s3', region_name=REGION_NAME)
        bucket_name = s3_bucket_path.split('/')[2]
        prefix = '/'.join(s3_bucket_path.split('/')[3:])
        
        # Listar archivos en el bucket
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', [])]
        
        if not files:
            print(f"No se encontraron archivos en la ruta S3: {s3_bucket_path}")
            return []
        
        # Filtrar solo los archivos que terminan en .csv
        csv_files = [file_key for file_key in files if file_key.endswith('.csv')]
        
        if not csv_files:
            print(f"No se encontraron archivos CSV en la ruta S3: {s3_bucket_path}")
            return []
        
        # Descargar solo los archivos CSV
        for file_key in csv_files:
            local_file_path = os.path.join(local_path, file_key.split('/')[-1])
            s3.download_file(bucket_name, file_key, local_file_path)
            print(f"Archivo CSV descargado: {local_file_path}")
        
        return csv_files
    except Exception as e:
        print(f"Error al descargar los archivos desde S3: {e}")
        return []

# Función para insertar datos en MySQL
def insert_into_mysql(df, table_name):
    try:
        # Crear la conexión a la base de datos especificando el nombre de la base de datos
        engine = create_engine(f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')
        
        # Usamos el parámetro chunksize para manejar grandes volúmenes de datos
        df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=1000)
        print(f"Datos insertados correctamente en la tabla {table_name} en MySQL.")
    except Exception as e:
        print(f"Error al insertar datos en MySQL: {e}")

# Función principal que coordina el flujo de trabajo
def main():
    # Crear la base de datos si no existe
    create_database_if_not_exists()

    # Verificar la conexión a MySQL
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        if connection.is_connected():
            print(f"Conexión exitosa a la base de datos '{MYSQL_DB}' en MySQL.")
        connection.close()
    except Error as e:
        print(f"Error al conectar a la base de datos {MYSQL_DB}: {e}")
        return

    # Descargar archivos CSV desde S3
    local_download_path = '/tmp/csv_files'
    os.makedirs(local_download_path, exist_ok=True)

    files = download_csv_from_s3(ATHENA_S3_OUTPUT, local_download_path)
    
    if not files:
        print("No se encontraron archivos CSV en el bucket.")
        return

    # Procesar cada archivo CSV y cargarlo en MySQL
    for file in files:
        local_file_path = os.path.join(local_download_path, file.split('/')[-1])

        try:
            # Cargar el archivo CSV en un DataFrame
            df = pd.read_csv(local_file_path)
            print(f"Archivo CSV {local_file_path} cargado exitosamente.")
            
            # Crear nombre de la tabla a partir del nombre del archivo
            table_name = f'table_from_{file.split("/")[-1].split(".")[0]}'
            
            # Insertar los datos en MySQL
            insert_into_mysql(df, table_name)
        except Exception as e:
            print(f"Error al procesar el archivo {local_file_path}: {e}")

if __name__ == "__main__":
    main()
