# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer un directorio de trabajo
WORKDIR /app

# Copiar el código fuente al contenedor
COPY . .

# Instalar las dependencias necesarias
# psycopg2-binary es la versión precompilada de psycopg2
RUN pip install confluent-kafka psycopg2-binary

# Comando para ejecutar la aplicación
CMD ["python", "productor.py"]
