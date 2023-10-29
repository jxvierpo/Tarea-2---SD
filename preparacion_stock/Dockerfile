# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer un directorio de trabajo
WORKDIR /app

# Copiar el archivo Python al contenedor
COPY . .

# Instalar las bibliotecas necesarias
RUN pip install confluent-kafka

# Comando para ejecutar el script Python cuando el contenedor se inicie
CMD ["python", "./preparacion_stock.py"]
