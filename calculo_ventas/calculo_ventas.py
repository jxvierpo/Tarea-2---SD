import json
import time
import smtplib
from email.message import EmailMessage
from confluent_kafka import Consumer, KafkaError
import psycopg2

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'ventas'


carrosRegistrados = set()
ventasTotales = {}

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['ventas'])

conn = psycopg2.connect(

    dbname="tarea2",
    user="postgres",
    password="postgres",
    host="postgres",  # Nombre del servicio en docker-compose
    port="5432"
    )
cursor = conn.cursor()

def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = 'reemplazatumail@gmail.com'  # Reemplaza con tu dirección de Gmail
    msg['To'] = to_email

    # Configuración del servidor SMTP para Gmail
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('reemplazatumail@gmail.com', 'tupass')  # Reemplaza con tu dirección de Gmail y contraseña
    server.send_message(msg)
    server.quit()


def procesar_mensaje(mensaje):
    data_str = mensaje.value().decode('utf-8')
    data = json.loads(data_str)

    patente = data['patente']
    email = data['email_vendedor']
    stock_restante = data['stock_restante']



    cursor.execute("SELECT SUM(cantidad) FROM ventas WHERE patente = %s ", (patente,))
    result = cursor.fetchone()
    total_ventas = result[0] if result else 0

    # Imprimir el total de ventas para esa patente
    email_body = (f"Total de ventas para el carro con patente {patente}: {total_ventas}." 
                  f"Stock restante {stock_restante}. ")
    send_email(email, "Registro de venta en MAMOCHI", email_body)
    

def poll_kafka():
    try:

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of topic {msg.topic()} partition {msg.partition()}")
                else:
                    print(f"Error while polling message: {msg.error()}")
            else:
                procesar_mensaje(msg)
    finally:
        consumer.close()
        cursor.close()
        conn.close()



if __name__ == "__main__":
    try:
        poll_kafka()
    except KeyboardInterrupt:
        print("\nResumen de ventas:")
        for patente, total in ventasTotales.items():
            print(f"Patente: {patente} - Ventas totales: {total}")
