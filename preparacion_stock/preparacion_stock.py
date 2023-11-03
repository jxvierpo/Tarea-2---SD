from confluent_kafka import Consumer, KafkaError
import psycopg2

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'stock'

conn = psycopg2.connect(
    dbname="tarea2",
    user="postgres",
    password="postgres",
    host="postgres",  # Nombre del servicio en docker-compose
    port="5432"
)
cursor = conn.cursor()

carrosRegistrados = set()
stockCarros = {}  # Diccionario para almacenar el stock de cada carro

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['stock'])

def procesar_mensaje(mensaje):
    data = eval(mensaje.value().decode('utf-8'))
    patente = data['patente']
    
    # Verificar si el carro está registrado
    cursor.execute("SELECT * FROM carros WHERE patente = %s", (patente,))
    if cursor.fetchone() is None:
        print(f"Error: Carro con patente {patente} no registrado.")
        return
    

    # Actualizar el stock del carro
    stock_adicional = data.get('stock_adicional', 0)
    cursor.execute("UPDATE carros SET stock = stock + %s WHERE patente = %s", (stock_adicional, patente))
    conn.commit()

    cursor.execute("SELECT stock FROM carros WHERE patente = %s", (patente,))
    stock_actualizado = cursor.fetchone()[0]
    print(f"Stock actualizado para el carro con patente {patente}. Stock total: {stock_actualizado}")


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
    poll_kafka()
