# Databricks notebook source
# MAGIC %md
# MAGIC # MVP — Pipeline CVM: Indicadores de Liquidez em Fundos
# MAGIC
# MAGIC Este notebook executa coleta, Bronze, Silver, Gold e preparação das análises.
# MAGIC Fonte: Portal Dados Abertos da CVM — Informe Diário.

# COMMAND ----------
# DBTITLE 1,1. Configuração
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import requests
import zipfile
import os
from datetime import datetime, timezone

CATALOG = "workspace"
SCHEMA = "cvm_liquidez"
BASE_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_informe_diario"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_informe_diario"
GOLD_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.gold_indicadores_liquidez_diarios"
GOLD_SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.gold_resumo_liquidez_fundo"

# Amostra: dois meses completos anteriores ao mês corrente.
MONTHS = ["202607", "202608"]

print("Catálogo:", CATALOG)
print("Schema:", SCHEMA)
print("Meses:", MONTHS)

# COMMAND ----------
# DBTITLE 2,2. Criar schema e Volume
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
print("Estrutura criada/verificada:", BASE_VOLUME)

# COMMAND ----------
# DBTITLE 3,3. Coletar arquivos da CVM
headers = {"User-Agent": "MVP-Academico-Databricks/1.0"}

for month in MONTHS:
    month_dir = os.path.join(BASE_VOLUME, month)
    os.makedirs(month_dir, exist_ok=True)

    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{month}.zip"
    local_zip = os.path.join(month_dir, f"inf_diario_fi_{month}.zip")
    local_csv = os.path.join(month_dir, f"inf_diario_fi_{month}.csv")

    if not os.path.exists(local_csv):
        response = requests.get(url, headers=headers, timeout=180)
        response.raise_for_status()
        with open(local_zip, "wb") as fp:
            fp.write(response.content)

        with zipfile.ZipFile(local_zip, "r") as zf:
            csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_members:
                raise ValueError(f"Nenhum CSV encontrado em {local_zip}")
            member = csv_members[0]
            with zf.open(member) as src, open(local_csv, "wb") as dst:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

        print("Baixado e extraído:", month)
    else:
        print("CSV já existente; mantendo para reprodutibilidade:", local_csv)

# COMMAND ----------
# DBTITLE 4,4. Ler CSVs e formar Bronze
input_paths = [os.path.join(BASE_VOLUME, m, f"inf_diario_fi_{m}.csv") for m in MONTHS]

raw_df = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("inferSchema", "false")
    .csv(input_paths)
)

# Metadados de ingestão, sem alterar as colunas da origem.
bronze_df = (
    raw_df
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingestion_ts", F.current_timestamp())
)

bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_TABLE)

print("Bronze gravada:", BRONZE_TABLE)
display(spark.table(BRONZE_TABLE).limit(10))

# COMMAND ----------
# DBTITLE 5,5. Silver — tipagem e qualidade básica
# O arquivo CVM utiliza ponto como separador decimal, então casts numéricos
# diretos são apropriados após a leitura do CSV como string.

silver_base = (
    spark.table(BRONZE_TABLE)
    .select(
        "TP_FUNDO", "CNPJ_FUNDO", "DT_COMPTC", "VL_TOTAL", "VL_QUOTA",
        "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST", "_source_file", "_ingestion_ts"
    )
    .withColumn("DT_COMPTC", F.to_date("DT_COMPTC"))
    .withColumn("VL_TOTAL", F.col("VL_TOTAL").cast("double"))
    .withColumn("VL_QUOTA", F.col("VL_QUOTA").cast("double"))
    .withColumn("VL_PATRIM_LIQ", F.col("VL_PATRIM_LIQ").cast("double"))
    .withColumn("CAPTC_DIA", F.col("CAPTC_DIA").cast("double"))
    .withColumn("RESG_DIA", F.col("RESG_DIA").cast("double"))
    .withColumn("NR_COTST", F.col("NR_COTST").cast("long"))
)

# Detecta duplicidades antes da deduplicação.
dup_keys = (
    silver_base
    .groupBy("CNPJ_FUNDO", "DT_COMPTC")
    .count()
    .withColumn("has_duplicate_key", F.col("count") > 1)
    .drop("count")
)

silver_with_flags = (
    silver_base
    .join(dup_keys, ["CNPJ_FUNDO", "DT_COMPTC"], "left")
    .withColumn("has_duplicate_key", F.coalesce(F.col("has_duplicate_key"), F.lit(False)))
    .withColumn(
        "is_valid_base",
        F.col("CNPJ_FUNDO").isNotNull()
        & F.col("DT_COMPTC").isNotNull()
        & F.col("VL_PATRIM_LIQ").isNotNull()
        & (F.col("VL_PATRIM_LIQ") > 0)
    )
)

# Deduplicação reprodutível pela chave natural e pelo momento de ingestão.
silver_df = (
    silver_with_flags
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("CNPJ_FUNDO", "DT_COMPTC").orderBy(F.col("_ingestion_ts").desc())
        )
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)

silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
print("Silver gravada:", SILVER_TABLE)
display(spark.table(SILVER_TABLE).limit(10))

# COMMAND ----------
# DBTITLE 6,6. Gold — indicadores diários
silver_valid = spark.table(SILVER_TABLE)

w = Window.partitionBy("CNPJ_FUNDO").orderBy("DT_COMPTC")

# O P95 é calculado sobre a amostra válida de todos os fundos/dias.
# Primeiro calculamos o PL do dia anterior.
with_d1 = (
    silver_valid
    .withColumn("vl_patrim_liq_d1", F.lag("VL_PATRIM_LIQ").over(w))
    .withColumn("fluxo_liquido", F.col("CAPTC_DIA") - F.col("RESG_DIA"))
    .withColumn(
        "taxa_resgate_pl",
        F.when(F.col("VL_PATRIM_LIQ") > 0, F.col("RESG_DIA") / F.col("VL_PATRIM_LIQ"))
    )
    .withColumn(
        "taxa_fluxo_liquido_pl",
        F.when(F.col("VL_PATRIM_LIQ") > 0, F.col("fluxo_liquido") / F.col("VL_PATRIM_LIQ"))
    )
    .withColumn(
        "taxa_resgate_sobre_pl_anterior",
        F.when(F.col("vl_patrim_liq_d1") > 0, F.col("RESG_DIA") / F.col("vl_patrim_liq_d1"))
    )
    .withColumn(
        "variacao_pl_d1",
        F.when(F.col("vl_patrim_liq_d1") > 0,
               F.col("VL_PATRIM_LIQ") / F.col("vl_patrim_liq_d1") - F.lit(1.0))
    )
)

p95 = (
    with_d1
    .filter(F.col("taxa_resgate_sobre_pl_anterior").isNotNull())
    .agg(F.expr("percentile_approx(taxa_resgate_sobre_pl_anterior, 0.95, 10000)").alias("p95"))
    .first()["p95"]
)

print("Percentil 95 da amostra:", p95)

if p95 is None:
    raise ValueError("Não foi possível calcular o percentil 95; verifique os dados válidos.")

gold_daily = (
    with_d1
    .withColumn("p95_amostra_taxa_resgate", F.lit(float(p95)))
    .withColumn(
        "evento_extremo_p95",
        F.when(
            F.col("taxa_resgate_sobre_pl_anterior").isNotNull()
            & (F.col("taxa_resgate_sobre_pl_anterior") > F.lit(float(p95))),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .select(
        "CNPJ_FUNDO", "TP_FUNDO", "DT_COMPTC",
        "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST",
        "fluxo_liquido", "taxa_resgate_pl", "taxa_fluxo_liquido_pl",
        "vl_patrim_liq_d1", "taxa_resgate_sobre_pl_anterior",
        "variacao_pl_d1", "p95_amostra_taxa_resgate", "evento_extremo_p95",
        "is_valid_base"
    )
)

gold_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DAILY_TABLE)
print("Gold diária gravada:", GOLD_DAILY_TABLE)
display(spark.table(GOLD_DAILY_TABLE).limit(10))

# COMMAND ----------
# DBTITLE 7,7. Gold — resumo por fundo

gold_summary = (
    spark.table(GOLD_DAILY_TABLE)
    .filter(F.col("is_valid_base") == True)
    .groupBy("CNPJ_FUNDO")
    .agg(
        F.count("*").alias("dias_validos"),
        F.min("DT_COMPTC").alias("data_inicial"),
        F.max("DT_COMPTC").alias("data_final"),
        F.avg("VL_PATRIM_LIQ").alias("pl_medio"),
        F.sum("CAPTC_DIA").alias("captacoes_periodo"),
        F.sum("RESG_DIA").alias("resgates_periodo"),
        F.sum("fluxo_liquido").alias("fluxo_liquido_periodo"),
        F.sum(F.when(F.col("fluxo_liquido") < 0, 1).otherwise(0)).alias("dias_fluxo_negativo"),
        F.sum("evento_extremo_p95").alias("qtd_eventos_extremos_p95")
    )
    .withColumn(
        "taxa_resgate_acumulada_rel_pl_medio",
        F.when(F.col("pl_medio") > 0, F.col("resgates_periodo") / F.col("pl_medio"))
    )
    .withColumn(
        "proporcao_dias_fluxo_negativo",
        F.when(F.col("dias_validos") > 0, F.col("dias_fluxo_negativo") / F.col("dias_validos"))
    )
    .orderBy(F.col("taxa_resgate_acumulada_rel_pl_medio").desc())
)

gold_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_SUMMARY_TABLE)
print("Resumo Gold gravado:", GOLD_SUMMARY_TABLE)
display(spark.table(GOLD_SUMMARY_TABLE).limit(20))

# COMMAND ----------
# DBTITLE 8,8. Validação final rápida
for table_name in [BRONZE_TABLE, SILVER_TABLE, GOLD_DAILY_TABLE, GOLD_SUMMARY_TABLE]:
    count = spark.table(table_name).count()
    print(f"{table_name}: {count:,} linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resultado esperado desta etapa
# MAGIC
# MAGIC Ao final devem existir quatro tabelas Delta:
# MAGIC
# MAGIC - `workspace.cvm_liquidez.bronze_informe_diario`
# MAGIC - `workspace.cvm_liquidez.silver_informe_diario`
# MAGIC - `workspace.cvm_liquidez.gold_indicadores_liquidez_diarios`
# MAGIC - `workspace.cvm_liquidez.gold_resumo_liquidez_fundo`
# MAGIC
# MAGIC Os resultados numéricos não são preenchidos antecipadamente para evitar qualquer invenção de valores.
