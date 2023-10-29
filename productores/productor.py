from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

KAFKA_BROKER = 'kafka:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

carrosRegistrados = set()

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def create_topics():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    topics = [
        {"topic": "ingreso", "num_partitions": 2, "replication_factor": 1},
        {"topic": "ventas", "num_partitions": 2, "replication_factor": 1},
        {"topic": "stock", "num_partitions": 2, "replication_factor": 1},
        {"topic": "avisos", "num_partitions": 2, "replication_factor": 1}
    ]
    admin_client.create_topics(topics)

create_topics()

def registro_miembro():
    patente = input("Introduce la patente del carro: ")
    if patente not in carrosRegistrados:
        nombre = input("Introduce el nombre del vendedor: ")
        premium = input("¿Es premium? (s/n): ")
        data = {"nombre": nombre, "patente": patente, "premium": premium.lower() == 's'}
        carrosRegistrados.add(patente)
        topic = 'ingreso'
        partition = 1 if data.get('premium') else 0
        producer.produce(topic, key=patente, value=str(data), partition=partition, callback=delivery_report)
        producer.flush()
        print(f"Vendedor {nombre} con patente {patente} registrado.")
    else:
        print("Error: carro ya se encuentra registrado")

def registro_venta():
    patente = input("Introduce la patente del carro: ")
    if patente in carrosRegistrados:
        cliente = input("Introduce el nombre del cliente: ")
        cantidad = input("Introduce la cantidad de mote con huesillo vendidos: ")
        data = {"cliente": cliente, "patente": patente, "cantidad": cantidad}
        producer.produce('ventas', key=patente, value=str(data), callback=delivery_report)
        producer.flush()
        print(f"Venta registrada para el cliente {cliente}.")
    else:
        print("Error: carro no registrado")

def main():
    while True:
        print("\n--- Menú ---")
        print("1. Registro de Vendedor")
        print("2. Registro de Venta de Mote con Huesillo")
        print("3. Salir")
        choice = input("Elige una opción: ")
        if choice == '1':
            registro_miembro()
        elif choice == '2':
            registro_venta()
        elif choice == '3':
            break

if __name__ == "__main__":
    main()
