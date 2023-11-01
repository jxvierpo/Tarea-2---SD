import json
import time
import smtplib
from email.message import EmailMessage
from confluent_kafka import Consumer, KafkaError

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

def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = 'reemplazacontumail@gmail.com'  # Reemplaza con tu dirección de Gmail
    msg['To'] = to_email

    # Configuración del servidor SMTP para Gmail
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('reemplazacontumail@gmail.com', 'tupass')  # Reemplaza con tu dirección de Gmail y contraseña
    server.send_message(msg)
    server.quit()


def procesar_mensaje(mensaje):
    data_str = mensaje.value().decode('utf-8')
    data = json.loads(data_str)

    patente = data['patente']
    email = data['email_vendedor']
    stock_restante = data['stock_restante']
    # Registrar la patente si no está en carrosRegistrados
    if patente not in carrosRegistrados:
        carrosRegistrados.add(patente)
        ventasTotales[patente] = 0

    # Sumar las ventas para esa patente
    ventasTotales[patente] += int(data['cantidad'])

    # Imprimir el total de ventas para esa patente
    email_body = (f"Total de ventas para el carro con patente {patente}: {ventasTotales[patente]}." 
                  f"Stock restante {stock_restante}. ")
    send_email(email, "Registro de venta en MAMOCHI", email_body)
    

def poll_kafka():
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

if __name__ == "__main__":
    try:
        poll_kafka()
    except KeyboardInterrupt:
        print("\nResumen de ventas:")
        for patente, total in ventasTotales.items():
            print(f"Patente: {patente} - Ventas totales: {total}")
