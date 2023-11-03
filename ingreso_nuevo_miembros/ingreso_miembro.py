import json
import time
import smtplib
import psycopg2
from email.message import EmailMessage
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'ingreso'

conn = psycopg2.connect(
    dbname="tarea2",
    user="postgres",
    password="postgres",
    host="postgres",  # Nombre del servicio en docker-compose
    port="5432"
)
cursor = conn.cursor()

lista_nuevos_miembros = []

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['ingreso'])


def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = 'javierigna.ahumada@mail.udp.cl'  # Reemplaza con tu dirección de Gmail
    msg['To'] = to_email

    # Configuración del servidor SMTP para Gmail
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('javierigna.ahumada@mail.udp.cl', 'javier123?')  # Reemplaza con tu dirección de Gmail y contraseña
    server.send_message(msg)
    server.quit()


    
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
            data_str = msg.value().decode('utf-8')
            data = json.loads(data_str)
            lista_nuevos_miembros.append(data)
            nombre = data['nombre']
            email = data['email']
            patente = data['patente']
            is_paid = data['paid']

            partition = msg.partition()

            if partition == 0:
                time.sleep(10)
                print(f"Nuevo registro de un miembro normal desde la partición {partition}: {data}")
            elif partition == 1:
                print(f"Nuevo registro de un miembro paid desde la partición {partition}: {data}")

            email_body = f"Hola {nombre}, te has registrado exitosamente con la patente {patente} como un miembro {'paid' if is_paid else 'normal'}."
            send_email(email, "Registro exitoso en MAMOCHI", email_body)

if __name__ == "__main__":
    poll_kafka()
