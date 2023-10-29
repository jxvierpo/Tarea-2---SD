from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'ventas'

carrosRegistrados = set()
registro = {}
resultados = {}

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['ventas'])

def procesar_mensaje(mensaje):
    data = eval(mensaje.value().decode('utf-8'))
    patente = data['patente']
    
    if patente not in carrosRegistrados:
        print(f"Error: Carro con patente {patente} no registrado.")
        return

    if patente not in registro:
        registro[patente] = [data]
    else:
        registro[patente].append(data)

def calcular_estadisticas():
    for patente, ventas in registro.items():
        contadorVentas = len(ventas)
        clientes = {venta['cliente'] for venta in ventas}
        cantidad = sum(int(venta['cantidad']) for venta in ventas)

        ventasTotales = contadorVentas
        clientesTotales = len(clientes)
        promedioVentas = cantidad / clientesTotales

        resultados[patente] = {
            'ventas_totales': ventasTotales,
            'clientes_totales': clientesTotales,
            'promedio_ventas': promedioVentas
        }

    for patente, stats in resultados.items():
        print(f"Carro: {patente}")
        print(f"Ventas totales: {stats['ventas_totales']}")
        print(f"Clientes totales: {stats['clientes_totales']}")
        print(f"Promedio de ventas: {stats['promedio_ventas']:.2f}")

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
    try:
        poll_kafka()
    except KeyboardInterrupt:
        calcular_estadisticas()
