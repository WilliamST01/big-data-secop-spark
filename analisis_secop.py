# -*- coding: utf-8 -*-
"""
Análisis en batch de contratos públicos (SECOP II) con Apache Spark.

Dataset: SECOP II - Contratos Electrónicos (datos.gov.co, id jbjy-vk9h)
Filtro de descarga: ciudades Puerto Salgar, La Dorada, Medellín y Cali.

Uso:
    python3 analisis_secop.py
    python3 analisis_secop.py <ruta_entrada> <carpeta_salida>
"""
import os
import sys

import matplotlib
matplotlib.use("Agg")  # sin interfaz gráfica: las figuras se guardan como PNG
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession, functions as F

RUTA_ENTRADA = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/Tarea3_secop/*.csv"
CARPETA_SALIDA = sys.argv[2] if len(sys.argv) > 2 else "resultados"
os.makedirs(CARPETA_SALIDA, exist_ok=True)

spark = (SparkSession.builder.appName("AnalisisSECOP")
         .config("spark.sql.shuffle.partitions", "8")  # pocas particiones: la VM tiene pocos recursos
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def titulo(texto):
    print("\n" + "=" * 70 + "\n" + texto + "\n" + "=" * 70)


# ---------------------------------------------------------------------------
# 1. CARGA DESDE LA FUENTE ORIGINAL (CSV en HDFS)
# ---------------------------------------------------------------------------
titulo("1. CARGA DE DATOS")
df_raw = (spark.read.format("csv")
          .option("header", "true")
          .option("multiLine", "true")   # los textos largos pueden traer saltos de línea
          .option("quote", '"')
          .option("escape", '"')
          .load(RUTA_ENTRADA))           # todo como texto; se convierte tipo a tipo abajo

# Cada tanda descargada trae su encabezado: se descartan las filas repetidas
df_raw = df_raw.filter(F.col("id_contrato") != "id_contrato")

print("Registros cargados:", df_raw.count())
df_raw.printSchema()

# ---------------------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACIÓN
# ---------------------------------------------------------------------------
titulo("2. LIMPIEZA Y TRANSFORMACIÓN")

df = (df_raw
      # tipos de dato
      .withColumn("valor_del_contrato", F.col("valor_del_contrato").cast("double"))
      .withColumn("fecha_de_firma", F.to_timestamp("fecha_de_firma"))
      .withColumn("fecha_de_inicio_del_contrato", F.to_timestamp("fecha_de_inicio_del_contrato"))
      .withColumn("fecha_de_fin_del_contrato", F.to_timestamp("fecha_de_fin_del_contrato"))
      # texto normalizado
      .withColumn("ciudad", F.initcap(F.trim("ciudad")))
      .withColumn("departamento", F.initcap(F.trim("departamento")))
      .withColumn("estado_contrato", F.trim("estado_contrato"))
      .withColumn("tipo_de_contrato", F.trim("tipo_de_contrato"))
      .withColumn("nombre_entidad", F.trim("nombre_entidad"))
      .withColumn("proveedor_adjudicado", F.trim("proveedor_adjudicado")))

total_antes = df.count()

# nulos y duplicados
df = (df.dropDuplicates(["id_contrato"])
        .dropna(subset=["id_contrato", "valor_del_contrato", "fecha_de_firma"])
        .filter(F.col("valor_del_contrato") >= 0))

# variables derivadas
df = (df.withColumn("anio", F.year("fecha_de_firma"))
        .withColumn("mes", F.date_format("fecha_de_firma", "yyyy-MM"))
        .withColumn("duracion_dias",
                    F.datediff("fecha_de_fin_del_contrato", "fecha_de_inicio_del_contrato")))

df.cache()
total_despues = df.count()
print("Registros antes de limpiar :", total_antes)
print("Registros después de limpiar:", total_despues)
print("Registros descartados       :", total_antes - total_despues)

# ---------------------------------------------------------------------------
# 3. ANÁLISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------------------
titulo("3. ANÁLISIS EXPLORATORIO")

print("\n-- Muestra de registros --")
df.select("nombre_entidad", "ciudad", "tipo_de_contrato", "estado_contrato",
          "valor_del_contrato", "fecha_de_firma").show(10, truncate=40)

print("\n-- Estadísticas del valor del contrato --")
df.select("valor_del_contrato").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

print("\n-- Valores nulos por columna --")
nulos = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
nulos.show(vertical=True)

print("\n-- Contratos y valor por ciudad --")
por_ciudad = (df.groupBy("ciudad")
              .agg(F.count("*").alias("contratos"),
                   F.sum("valor_del_contrato").alias("valor_total"),
                   F.avg("valor_del_contrato").alias("valor_promedio"))
              .orderBy(F.desc("valor_total")))
por_ciudad.show(truncate=False)

print("\n-- Contratos por tipo de contrato --")
por_tipo = (df.groupBy("tipo_de_contrato")
            .agg(F.count("*").alias("contratos"), F.sum("valor_del_contrato").alias("valor_total"))
            .orderBy(F.desc("contratos")))
por_tipo.show(15, truncate=False)

print("\n-- Contratos por estado --")
por_estado = df.groupBy("estado_contrato").count().orderBy(F.desc("count"))
por_estado.show(truncate=False)

print("\n-- Top 10 entidades por valor contratado --")
top_entidades = (df.groupBy("nombre_entidad")
                 .agg(F.count("*").alias("contratos"), F.sum("valor_del_contrato").alias("valor_total"))
                 .orderBy(F.desc("valor_total")).limit(10))
top_entidades.show(truncate=50)

print("\n-- Top 10 proveedores por número de contratos --")
top_proveedores = (df.groupBy("proveedor_adjudicado")
                   .agg(F.count("*").alias("contratos"), F.sum("valor_del_contrato").alias("valor_total"))
                   .orderBy(F.desc("contratos")).limit(10))
top_proveedores.show(truncate=50)

print("\n-- Evolución mensual --")
por_mes = (df.groupBy("mes")
           .agg(F.count("*").alias("contratos"), F.sum("valor_del_contrato").alias("valor_total"))
           .orderBy("mes"))
por_mes.show(24)

# ---------------------------------------------------------------------------
# 4. VISUALIZACIÓN (los resultados agregados son pequeños: se pasan a pandas)
# ---------------------------------------------------------------------------
titulo("4. VISUALIZACIÓN")
AZUL, ROJO = "#1F3864", "#B0060F"


def guardar(nombre):
    plt.tight_layout()
    ruta = os.path.join(CARPETA_SALIDA, nombre)
    plt.savefig(ruta, dpi=150)
    plt.close()
    print("Gráfico guardado:", ruta)


p = por_ciudad.toPandas()
plt.figure(figsize=(8, 5))
plt.bar(p["ciudad"], p["valor_total"] / 1e9, color=AZUL)
plt.title("Valor total contratado por ciudad")
plt.ylabel("Miles de millones de COP")
guardar("1_valor_por_ciudad.png")

p = por_tipo.limit(8).toPandas()
plt.figure(figsize=(9, 5))
plt.barh(p["tipo_de_contrato"][::-1], p["contratos"][::-1], color=ROJO)
plt.title("Número de contratos por tipo de contrato")
plt.xlabel("Contratos")
guardar("2_contratos_por_tipo.png")

p = por_estado.toPandas()
plt.figure(figsize=(8, 5))
plt.bar(p["estado_contrato"], p["count"], color=AZUL)
plt.title("Contratos por estado")
plt.xticks(rotation=30, ha="right")
plt.ylabel("Contratos")
guardar("3_contratos_por_estado.png")

p = top_entidades.toPandas()
plt.figure(figsize=(10, 5))
plt.barh(p["nombre_entidad"].str.slice(0, 45)[::-1], p["valor_total"][::-1] / 1e9, color=ROJO)
plt.title("Top 10 entidades por valor contratado")
plt.xlabel("Miles de millones de COP")
guardar("4_top_entidades.png")

p = por_mes.toPandas()
plt.figure(figsize=(10, 5))
plt.plot(p["mes"], p["contratos"], marker="o", color=AZUL)
plt.title("Contratos firmados por mes")
plt.xticks(p["mes"][::max(1, len(p) // 12)], rotation=45, ha="right")
plt.ylabel("Contratos")
guardar("5_evolucion_mensual.png")

titulo("FIN DEL ANÁLISIS")
spark.stop()
