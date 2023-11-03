# Usamos una imagen base de Python 3.8
FROM python:3.9-slim

# Establecemos un directorio de trabajo
WORKDIR /app

# Copiamos los requerimientos para instalar las bibliotecas necesarias
COPY . .

# Instalamos las bibliotecas necesarias
RUN pip install confluent-kafka psycopg2-binary




# Establecemos el comando para ejecutar el script cuando se inicie el contenedor
CMD ["python", "ingreso_miembro.py"]
