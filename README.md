# Análisis de contratación pública (SECOP II) con Apache Spark

Proyecto de la **Tarea 3 – Procesamiento de Datos con Apache Spark** del curso Big Data (código 202016911), Universidad Nacional Abierta y a Distancia (UNAD).

**Autor:** William Steven Hernandez Rueda · Grupo 202016911_25

## Problema

Entender cómo se distribuyen los contratos públicos y sus valores por ciudad, entidad, tipo de contrato y estado en Puerto Salgar, La Dorada, Medellín y Cali, y cómo evolucionan en el tiempo.

## Conjunto de datos

- **Fuente:** [datos.gov.co](https://www.datos.gov.co), dataset *SECOP II – Contratos Electrónicos* (id `jbjy-vk9h`).
- **Filtro:** ciudades Puerto Salgar, La Dorada, Medellín y Cali.
- **Variables (17):** nombre_entidad, nit_entidad, departamento, ciudad, id_contrato, estado_contrato, descripcion_del_proceso, tipo_de_contrato, fecha_de_firma, fecha_de_inicio_del_contrato, fecha_de_fin_del_contrato, tipodocproveedor, documento_proveedor, proveedor_adjudicado, valor_del_contrato, objeto_del_contrato, duraci_n_del_contrato.
- El CSV no se incluye en el repositorio por su tamaño; se descarga con los pasos de abajo.

## Estructura

```
.
├── analisis_secop.py              # Procesamiento en batch con Spark (carga, limpieza, EDA, gráficos)
├── streaming/
│   ├── kafka_producer.py          # Productor de datos simulados (Kafka)
│   └── spark_streaming_consumer.py# Consumidor con Spark Streaming
├── requirements.txt
└── README.md
```

## Requisitos

Entorno usado: máquina virtual Ubuntu Server 22.04 con Hadoop 3.3.6 (HDFS) y Apache Spark, Python 3.

```bash
pip install -r requirements.txt
```

## Ejecución del procesamiento en batch

1. Iniciar Hadoop: `start-all.sh`
2. Descargar los datos desde la API de datos.gov.co en tandas de 50.000 filas, unirlos en `Tarea3_secop.csv` y subirlos a HDFS:

   ```bash
   BASE="https://www.datos.gov.co/resource/jbjy-vk9h.csv"
   SEL="nombre_entidad,nit_entidad,departamento,ciudad,id_contrato,estado_contrato,descripcion_del_proceso,tipo_de_contrato,fecha_de_firma,fecha_de_inicio_del_contrato,fecha_de_fin_del_contrato,tipodocproveedor,documento_proveedor,proveedor_adjudicado,valor_del_contrato,objeto_del_contrato,duraci_n_del_contrato"
   WHERE="upper(ciudad)%20in(%27LA%20DORADA%27,%27PUERTO%20SALGAR%27,%27MEDELL%C3%8DN%27,%27CALI%27)"

   mkdir -p secop && cd secop
   for OFFSET in 0 50000 100000; do
     wget -O parte_$OFFSET.csv "$BASE?\$select=$SEL&\$where=$WHERE&\$order=id_contrato&\$limit=50000&\$offset=$OFFSET"
   done
   head -1 parte_0.csv > Tarea3_secop.csv
   for f in parte_*.csv; do tail -n +2 "$f" >> Tarea3_secop.csv; done
   hdfs dfs -put Tarea3_secop.csv /Tarea3/
   ```

3. Ejecutar el análisis:

   ```bash
   python3 analisis_secop.py "hdfs://localhost:9000/Tarea3/Tarea3_secop.csv"
   ```

### Qué hace el script

1. **Carga** el CSV desde HDFS.
2. **Limpia y transforma:** convierte tipos (valor numérico, fechas), normaliza texto, elimina duplicados por `id_contrato` y registros sin valor o sin fecha, y crea las variables `anio`, `mes` y `duracion_dias`.
3. **Análisis exploratorio:** estadísticas del valor del contrato, nulos por columna, y agregaciones por ciudad, tipo de contrato, estado, entidad, proveedor y mes.
4. **Visualiza:** guarda cinco gráficos PNG en `resultados/`.

## Procesamiento en tiempo real (Kafka + Spark Streaming)

Basado en el Anexo 3 del curso, con datos simulados de sensores.

```bash
# 1. Iniciar ZooKeeper y Kafka, y crear el topic
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic sensor_data

# 2. Terminal 1: productor
python3 streaming/kafka_producer.py

# 3. Terminal 2: consumidor (ajustar la versión del paquete a la de Spark instalada: spark-submit --version)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  streaming/spark_streaming_consumer.py
```
