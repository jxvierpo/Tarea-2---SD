from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'stock'

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
    
    if patente not in carrosRegistrados:
        print(f"Error: Carro con patente {patente} no registrado.")
        return

    # Actualizar el stock del carro
    stock_adicional = data.get('stock_adicional', 0)
    if patente not in stockCarros:
        stockCarros[patente] = stock_adicional
    else:
        stockCarros[patente] += stock_adicional

    print(f"Stock actualizado para el carro con patente {patente}. Stock total: {stockCarros[patente]}")

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
