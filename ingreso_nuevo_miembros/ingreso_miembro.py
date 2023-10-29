from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'ingreso'

lista_nuevos_miembros = []

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['ingreso'])

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
            data = msg.value().decode('utf-8')
            lista_nuevos_miembros.append(data)
            partition = msg.partition()
            if partition == 0:
                print(f"Nuevo registro de un miembro normal desde la partición {partition}: {data}")
            elif partition == 1:
                print(f"Nuevo registro de un miembro premium desde la partición {partition}: {data}")

if __name__ == "__main__":
    poll_kafka()
