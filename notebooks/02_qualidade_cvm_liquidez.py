# Databricks notebook source
# MAGIC %md
# MAGIC # Qualidade de Dados — MVP CVM Liquidez

# COMMAND ----------
from pyspark.sql import functions as F

SILVER = "workspace.cvm_liquidez.silver_informe_diario"
BRONZE = "workspace.cvm_liquidez.bronze_informe_diario"
GOLD = "workspace.cvm_liquidez.gold_indicadores_liquidez_diarios"

# COMMAND ----------
# DBTITLE 1,Completude da Silver
silver = spark.table(SILVER)

completude = silver.agg(
    F.count("*").alias("total_linhas"),
    F.sum(F.when(F.col("CNPJ_FUNDO").isNull(), 1).otherwise(0)).alias("cnpj_nulo"),
    F.sum(F.when(F.col("DT_COMPTC").isNull(), 1).otherwise(0)).alias("data_nula"),
    F.sum(F.when(F.col("VL_PATRIM_LIQ").isNull(), 1).otherwise(0)).alias("pl_nulo"),
    F.sum(F.when(F.col("CAPTC_DIA").isNull(), 1).otherwise(0)).alias("captacao_nula"),
    F.sum(F.when(F.col("RESG_DIA").isNull(), 1).otherwise(0)).alias("resgate_nulo")
)
display(completude)

# COMMAND ----------
# DBTITLE 2,Duplicidades sinalizadas antes da deduplicação
bronze = spark.table(BRONZE)

# Recalcula a duplicidade da chave natural no bruto.
duplicidades = (
    bronze.groupBy("CNPJ_FUNDO", "DT_COMPTC")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.col("count").desc())
)
print("Quantidade de chaves duplicadas no Bronze:", duplicidades.count())
display(duplicidades.limit(20))

# COMMAND ----------
# DBTITLE 3,Consistência — valores negativos nas métricas que deveriam ser não negativas
negativos = silver.agg(
    F.sum(F.when(F.col("VL_PATRIM_LIQ") < 0, 1).otherwise(0)).alias("pl_negativo"),
    F.sum(F.when(F.col("CAPTC_DIA") < 0, 1).otherwise(0)).alias("captacao_negativa"),
    F.sum(F.when(F.col("RESG_DIA") < 0, 1).otherwise(0)).alias("resgate_negativo"),
    F.sum(F.when(F.col("NR_COTST") < 0, 1).otherwise(0)).alias("cotistas_negativos")
)
display(negativos)

# COMMAND ----------
# DBTITLE 4,Registros não válidos para os indicadores principais
invalidos = (
    silver.filter(~F.col("is_valid_base"))
    .select("CNPJ_FUNDO", "DT_COMPTC", "VL_PATRIM_LIQ", "is_valid_base")
)
print("Registros não válidos para os indicadores principais:", invalidos.count())
display(invalidos.limit(20))

# COMMAND ----------
# DBTITLE 5,Outliers — maiores taxas de resgate sobre PL anterior
outliers = (
    spark.table(GOLD)
    .filter(F.col("taxa_resgate_sobre_pl_anterior").isNotNull())
    .select(
        "CNPJ_FUNDO", "DT_COMPTC", "RESG_DIA",
        "vl_patrim_liq_d1", "taxa_resgate_sobre_pl_anterior",
        "evento_extremo_p95"
    )
    .orderBy(F.col("taxa_resgate_sobre_pl_anterior").desc())
)
display(outliers.limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Interpretação
# MAGIC
# MAGIC O objetivo da qualidade é mostrar que completude, consistência, unicidade e outliers foram verificadas. Extremos não são excluídos automaticamente; são preservados e analisados.
