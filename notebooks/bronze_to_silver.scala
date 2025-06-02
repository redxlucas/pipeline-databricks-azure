// Databricks notebook source
// MAGIC %python
// MAGIC
// MAGIC dbutils.fs.ls("/mnt/data/bronze")

// COMMAND ----------

// MAGIC %md
// MAGIC # Lendo os dados na camada de bronze

// COMMAND ----------

val path = "dbfs:/mnt/data/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC # Transformando os campos do JSON em colunas

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*", "anuncio.endereco.*")
  )

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")
display(dados_detalhados)

// COMMAND ----------

// MAGIC %md
// MAGIC # Removendo colunas

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)

// COMMAND ----------

// MAGIC %md
// MAGIC # Salvando na camada silver

// COMMAND ----------

val path = "dbfs:/mnt/data/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)
