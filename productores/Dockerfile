# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer un directorio de trabajo
WORKDIR /app

# Copiar el código fuente al contenedor
COPY . .

RUN pip install confluent-kafka



# Comando para ejecutar la aplicación
CMD ["python", "productor.py"]
