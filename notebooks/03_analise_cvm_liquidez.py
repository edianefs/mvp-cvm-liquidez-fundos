# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Análise Final — Perguntas do MVP
# MAGIC
# MAGIC Este notebook responde P1, P2 e P3 usando apenas as tabelas Gold produzidas pelo pipeline.

# COMMAND ----------

GOLD_DAILY = "workspace.cvm_liquidez.gold_indicadores_liquidez_diarios"
GOLD_SUMMARY = "workspace.cvm_liquidez.gold_resumo_liquidez_fundo"

# COMMAND ----------

# DBTITLE 1,P1 — maiores taxas acumuladas de resgate relativas ao PL médio
p1 = spark.sql(f"""
SELECT
  CNPJ_FUNDO_CLASSE,
  ID_SUBCLASSE,
  resgates_periodo,
  pl_medio,
  taxa_resgate_acumulada_rel_pl_medio,
  data_inicial,
  data_final
FROM {GOLD_SUMMARY}
WHERE pl_medio > 0
ORDER BY taxa_resgate_acumulada_rel_pl_medio DESC
LIMIT 20
""")

display(p1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretação de P1
# MAGIC
# MAGIC A métrica `taxa_resgate_acumulada_rel_pl_medio` representa a razão entre os resgates acumulados no período e o PL médio observado. Por ser acumulada, não representa o percentual do patrimônio resgatado em um único evento.

# COMMAND ----------

# DBTITLE 2,P2 — frequência de dias de fluxo líquido negativo
p2 = spark.sql(f"""
SELECT
  CNPJ_FUNDO_CLASSE,
  ID_SUBCLASSE,
  dias_validos,
  dias_fluxo_negativo,
  proporcao_dias_fluxo_negativo,
  fluxo_liquido_periodo
FROM {GOLD_SUMMARY}
WHERE dias_validos > 0
ORDER BY proporcao_dias_fluxo_negativo DESC, dias_fluxo_negativo DESC
LIMIT 20
""")

display(p2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretação de P2
# MAGIC
# MAGIC A `proporcao_dias_fluxo_negativo` mede a proporção de dias válidos em que o fluxo líquido (`CAPTC_DIA - RESG_DIA`) foi negativo. Valores elevados indicam recorrência de saídas líquidas no período analisado.

# COMMAND ----------

# DBTITLE 3,P3 — eventos extremos acima do P95 da amostra
p3 = spark.sql(f"""
SELECT
  CNPJ_FUNDO_CLASSE,
  ID_SUBCLASSE,
  SUM(evento_extremo_p95) AS qtd_eventos_extremos_p95,
  COUNT(CASE WHEN taxa_resgate_sobre_pl_anterior IS NOT NULL THEN 1 END) AS dias_com_denominador_d1,
  MAX(p95_amostra_taxa_resgate) AS p95_amostra,
  CASE WHEN COUNT(CASE WHEN taxa_resgate_sobre_pl_anterior IS NOT NULL THEN 1 END) > 0
       THEN SUM(evento_extremo_p95) / COUNT(CASE WHEN taxa_resgate_sobre_pl_anterior IS NOT NULL THEN 1 END)
       ELSE NULL END AS proporcao_eventos_extremos
FROM {GOLD_DAILY}
WHERE is_valid_base = true
GROUP BY CNPJ_FUNDO_CLASSE, ID_SUBCLASSE
ORDER BY qtd_eventos_extremos_p95 DESC, proporcao_eventos_extremos DESC
LIMIT 20
""")

display(p3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretação de P3
# MAGIC
# MAGIC O `evento_extremo_p95` identifica observações cuja taxa de resgate sobre o PL anterior supera o P95 da distribuição amostral. No período analisado, o P95 foi aproximadamente 0,43%. Esse valor é um limiar estatístico construído para o MVP e não representa limite ou parâmetro regulatório da CVM.

# COMMAND ----------

# DBTITLE 4,Série temporal de um fundo selecionado
CNPJ_EXEMPLO = "52.984.696/0001-31"

serie = spark.sql(f"""
SELECT
  DT_COMPTC,
  VL_PATRIM_LIQ,
  CAPTC_DIA,
  RESG_DIA,
  fluxo_liquido,
  taxa_resgate_pl,
  taxa_resgate_sobre_pl_anterior,
  evento_extremo_p95
FROM {GOLD_DAILY}
WHERE CNPJ_FUNDO_CLASSE = '{CNPJ_EXEMPLO}'
ORDER BY DT_COMPTC
""")

display(serie)

# COMMAND ----------

# MAGIC %md
# MAGIC # Síntese e limitações dos resultados
# MAGIC
# MAGIC - A análise cobre os dados disponíveis de 01/07/2026 a 31/08/2026.
# MAGIC - Os indicadores são descritivos e dependem da qualidade e estrutura dos dados de origem.
# MAGIC - Valores extremos foram preservados e sinalizados, não excluídos automaticamente.
# MAGIC - `ID_SUBCLASSE` apresenta alta incidência de valores nulos na fonte e foi tratado sem preenchimento artificial.
# MAGIC - O CNPJ da classe/fundo é o principal identificador público utilizado nas análises, com `ID_SUBCLASSE` mantido quando disponível.
# MAGIC - Os resultados não constituem avaliação regulatória, recomendação de investimento ou conclusão definitiva sobre risco de liquidez de qualquer fundo.
