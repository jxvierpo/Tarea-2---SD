from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'stock'

carrosRegistrados = set()
contadorVentas = {}

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

    if patente not in contadorVentas:
        contadorVentas[patente] = 1
    else:
        contadorVentas[patente] += 1

    if contadorVentas[patente] == 5 or int(data['stock_restante']) < 20:
        print(f'Carro {patente} necesita reponer Stock!')
        contadorVentas[patente] = 0

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
            partition = msg.partition()
            if partition == 0:
                print(f"Nuevo registro de un miembro normal desde la partición {partition}")
            elif partition == 1:
                print(f"Nuevo registro de un miembro premium desde la partición {partition}")
            procesar_mensaje(msg)

if __name__ == "__main__":
    poll_kafka()
