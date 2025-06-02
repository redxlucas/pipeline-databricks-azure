// Databricks notebook source
// MAGIC %md
// MAGIC # Conferindo se os dados foram montados e se temos acesso a pasta Inbound

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/data/inbound")

// COMMAND ----------

// MAGIC %md
// MAGIC # Lendo os dados na camada Inbound

// COMMAND ----------

val path = "dbfs:/mnt/data/inbound/dados_brutos_imoveis.json"
val data = spark.read.json(path)

// COMMAND ----------

display(data)

// COMMAND ----------

// MAGIC %md
// MAGIC # Removendo colunas
// MAGIC Não nos interessa as colunas de imagens e usuários, por isso, vamos excluí-las

// COMMAND ----------

val announce_data = data.drop("imagens", "usuario")
display(announce_data)

// COMMAND ----------

// MAGIC %md
// MAGIC # Criando uma coluna de identificação

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = announce_data.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC # Salvando na camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/data/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)
