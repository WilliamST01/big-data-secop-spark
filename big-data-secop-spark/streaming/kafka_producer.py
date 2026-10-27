# -*- coding: utf-8 -*-
"""Productor de Kafka: genera datos simulados de sensores y los envía al topic sensor_data."""
import json
import random
import time

from kafka import KafkaProducer


def generar_dato_sensor():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time()),  # segundos desde 1970
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

while True:
    dato = generar_dato_sensor()
    producer.send("sensor_data", value=dato)
    print(f"Enviado: {dato}")
    time.sleep(1)
