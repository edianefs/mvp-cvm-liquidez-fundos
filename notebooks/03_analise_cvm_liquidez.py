# Databricks notebook source
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
# MAGIC **Como responder P1 no texto:** mencionar os fundos no topo da tabela e descrever a taxa observada. Não chamar o resultado de “alto risco regulatório”.

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
# MAGIC **Como responder P2 no texto:** destacar os fundos com maior proporção de dias de fluxo líquido negativo e explicar que a métrica é uma sinalização de recorrência de saída líquida, não uma conclusão regulatória.

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
# MAGIC **Como responder P3 no texto:** explicar que o P95 foi obtido da própria distribuição amostral e serve para localizar eventos relativamente extremos. Não interpretar o P95 como limite oficial da CVM.

# COMMAND ----------
# DBTITLE 4,Exemplo de série temporal de um fundo
# Preencha temporariamente um CNPJ_FUNDO_CLASSE observado nas tabelas acima.
CNPJ_EXEMPLO = ""

if CNPJ_EXEMPLO:
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
else:
    print("Defina CNPJ_EXEMPLO apenas após escolher um fundo real retornado pelas consultas.")
