import json
import psycopg2
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic
from confluent_kafka.admin import AdminClient
from random import randint


KAFKA_BROKER = 'kafka:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

conn = psycopg2.connect(
    dbname="tarea2",
    user="postgres",
    password="postgres",
    host="postgres",  # Nombre del servicio en docker-compose
    port="5432"
)
cursor = conn.cursor()

# Crear tabla si no existe
cursor.execute("""
CREATE TABLE IF NOT EXISTS carros (
    patente INT PRIMARY KEY,
    nombre VARCHAR(255),
    email VARCHAR(255),
    stock INT,
    paid BOOLEAN
)
""")
# Crear tabla de ventas si no existe
cursor.execute("""
CREATE TABLE IF NOT EXISTS ventas (
    id SERIAL PRIMARY KEY,
    patente INT,
    cantidad INT DEFAULT 1,
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (patente) REFERENCES carros(patente)
)
""")
conn.commit()

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
        NewTopic("stock", num_partitions=2, replication_factor=1)
    ]

    admin_client.create_topics(topics)

create_topics()

def registro_miembro(patente):
    #patente = randint(1,1000)
    cursor.execute("SELECT * FROM carros WHERE patente = %s", (patente,))
    if cursor.fetchone() is None:

        nombre = f"vendedor mote {patente}"
        email = "necal85927@soebing.com"
        
        paid = "s"
        data = {"nombre": nombre, "email": email, "patente": patente, "stock_restante": STOCK_INICIAL, "paid": paid.lower() == 's'}
        data_str = json.dumps(data)
        
        topic = 'ingreso'
        partition = 1 if data.get('paid') else 0
        producer.produce(topic, key=str(patente), value=str(data_str), partition=partition, callback=delivery_report)
        producer.flush()
        print(f"Vendedor {nombre} con patente {patente} registrado.")
        cursor.execute("INSERT INTO carros (patente, nombre, email, stock, paid) VALUES (%s, %s, %s, %s, %s)", (patente, nombre, email, STOCK_INICIAL, paid.lower() == 's'))
    else:
        print("Error: carro ya se encuentra registrado")

def registro_venta(patente):
    cursor.execute("SELECT * FROM carros WHERE patente = %s", (patente,))
    carro = cursor.fetchone()
    if carro:
        cliente = f"Comprador en carro {patente}"
        stock_actual = carro[3]
        if stock_actual == 0:
            print(f"El carro con patente {patente} se ha quedado sin stock!")
            print(f"Solicitando Stock +10 unidades")
            solicitar_stock(patente)
            return
        cursor.execute("UPDATE carros SET stock = stock - 1 WHERE patente = %s", (patente,))
        conn.commit()
        email_vendedor = carro[2]
        data = {"cliente": cliente, "patente": patente, "stock_restante": stock_actual - 1, "email_vendedor": email_vendedor}
        data_str = json.dumps(data)
        producer.produce('ventas', key=str(patente), value=str(data_str), callback=delivery_report)
        producer.flush()
        print(f"Venta registrada para el cliente {cliente}.")
        # Insertar la venta en la tabla 'ventas'
        cursor.execute("INSERT INTO ventas (patente) VALUES (%s)", (patente,))
        conn.commit()
    else:
        print("Error: carro no registrado")
    

def solicitar_stock(patente):
    cursor.execute("SELECT * FROM carros WHERE patente = %s", (patente,))
    carro = cursor.fetchone()
    if carro:
        cursor.execute("UPDATE carros SET stock = stock + 10 WHERE patente = %s", (patente,))
        conn.commit()

        cursor.execute("SELECT stock FROM carros WHERE patente = %s", (patente,))
        stock_total = cursor.fetchone()[0]  # Aquí obtienes el stock actualizado

        data = {"patente": patente, "stock_adicional": 10, "stock_total": stock_total}

        producer.produce('stock', key=str(patente), value=json.dumps(data), callback=delivery_report)
        producer.flush()
        print(f"Stock añadido para el carro con patente {patente}. Stock total: {stock_total}")
        
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
            patente = int(input("Ingrese la patente del vendedor: "))
            registro_miembro(patente)
        elif choice == '2':
            patente = int(input("Ingrese la patente del vendedor: "))
            registro_venta(patente)
        elif choice == '3':
            patente = int(input("Ingrese la patente del vendedor: "))
            solicitar_stock(patente)
        elif choice == '4':
            break

if __name__ == "__main__":
    main()
