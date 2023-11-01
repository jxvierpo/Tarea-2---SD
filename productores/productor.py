import json
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic
from confluent_kafka.admin import AdminClient

KAFKA_BROKER = 'kafka:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

carrosRegistrados = set()
stockCarros = {}
STOCK_INICIAL = 10

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def create_topics():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    topics = [
        NewTopic("ingreso", num_partitions=2, replication_factor=1),
        NewTopic("ventas", num_partitions=2, replication_factor=1),
        NewTopic("stock", num_partitions=2, replication_factor=1),
        NewTopic("avisos", num_partitions=2, replication_factor=1)
    ]

    admin_client.create_topics(topics)

create_topics()

def registro_miembro():
    patente = input("Introduce la patente del carro: ")
    if patente not in carrosRegistrados:
        nombre = input("Introduce el nombre del vendedor: ")
        email = input("Introduce el email del vendedor: ")
        stockCarros[patente] = STOCK_INICIAL
        paid = input("¿Es paid? (s/n): ")
        data = {"nombre": nombre, "email": email, "patente": patente, "stock_restante": STOCK_INICIAL, "paid": paid.lower() == 's'}
        data_str = json.dumps(data)
        carrosRegistrados.add(patente)
        topic = 'ingreso'
        partition = 1 if data.get('paid') else 0
        producer.produce(topic, key=patente, value=str(data_str), partition=partition, callback=delivery_report)
        producer.flush()
        print(f"Vendedor {nombre} con patente {patente} registrado.")
        stockCarros[patente] = {"stock": STOCK_INICIAL, "email": email}
    else:
        print("Error: carro ya se encuentra registrado")

def registro_venta():
    patente = input("Introduce la patente del carro: ")
    if patente in carrosRegistrados:
        cliente = input("Introduce el nombre del cliente: ")
        cantidad = int(input("Introduce la cantidad de mote con huesillo vendidos: "))

        if stockCarros[patente]["stock"] < cantidad:
            print("Error: Stock insuficiente para este carro.")
            return
        
        stockCarros[patente]["stock"] -= cantidad

        if stockCarros[patente]["stock"] == 0:
            print(f"El carro con patente {patente} se ha quedado sin stock!")

        email_vendedor = stockCarros[patente]["email"]
        data = {"cliente": cliente, "patente": patente, "cantidad": cantidad, "stock_restante": stockCarros[patente]["stock"], "email_vendedor": email_vendedor}
        data_str = json.dumps(data)
        producer.produce('ventas', key=patente, value=str(data_str), callback=delivery_report)
        producer.flush()
        print(f"Venta registrada para el cliente {cliente}.")
    else:
        print("Error: carro no registrado")

def solicitar_stock():
    patente = input("Introduce la patente del carro para el cual deseas agregar stock: ")
    if patente in carrosRegistrados:
        cantidad_adicional = int(input("Introduce la cantidad de stock adicional: "))
        
        # Aumentar el stock
        stockCarros[patente]["stock"] += cantidad_adicional

        data = {"patente": patente, "stock_adicional": cantidad_adicional, "stock_total": stockCarros[patente]["stock"]}
        producer.produce('stock', key=patente, value=str(data), callback=delivery_report)
        producer.flush()
        print(f"Stock añadido para el carro con patente {patente}. Stock total: {stockCarros[patente]}")
    else:
        print("Error: carro no registrado. Por favor, registra el carro primero.")


def main():
    while True:
        print("\n--- Menú ---")
        print("1. Registro de Vendedor")
        print("2. Registro de Venta de Mote con Huesillo")
        print("3. Solicitar Stock")
        print("4. Salir")
        choice = input("Elige una opción: ")
        if choice == '1':
            registro_miembro()
        elif choice == '2':
            registro_venta()
        elif choice == '3':
            solicitar_stock()
        elif choice == '4':
            break

if __name__ == "__main__":
    main()
