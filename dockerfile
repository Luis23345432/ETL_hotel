# Usar una imagen base oficial de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar todos los archivos del directorio actual al contenedor
COPY . /app

# Instalar las dependencias de Python especificadas en el archivo requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto que se usará (asumiendo que tu aplicación lo usa)
EXPOSE 8005

# Definir el comando para ejecutar la aplicación
CMD ["python", "etl_hotel.py"]
