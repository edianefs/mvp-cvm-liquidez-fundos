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
CNPJ_EXEMPLO = "52.984.696/0001-31"

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

# COMMAND ----------

# MAGIC %md
# MAGIC # Síntese e limitações dos resultados
# MAGIC
# MAGIC ## P1 — Resgates acumulados em relação ao PL médio
# MAGIC
# MAGIC O indicador `taxa_resgate_acumulada_rel_pl_medio` representa a razão entre os resgates acumulados no período e o PL médio observado. Por ser acumulado, não deve ser interpretado como percentual do patrimônio resgatado em um único evento nem como limite regulatório.
# MAGIC
# MAGIC ## P2 — Recorrência de fluxo líquido negativo
# MAGIC
# MAGIC A `proporcao_dias_fluxo_negativo` mede a proporção de dias válidos em que o fluxo líquido (`CAPTC_DIA - RESG_DIA`) foi negativo. Valores elevados indicam recorrência de saídas líquidas no período analisado, sem constituir, isoladamente, conclusão sobre risco regulatório.
# MAGIC
# MAGIC ## P3 — Eventos extremos
# MAGIC
# MAGIC O `evento_extremo_p95` identifica observações cuja taxa de resgate sobre o PL anterior supera o P95 da distribuição amostral. No período analisado, o P95 foi aproximadamente 0,43%. Esse valor é um limiar estatístico construído para o MVP e não representa limite ou parâmetro regulatório da CVM.
# MAGIC
# MAGIC ## Limitações
# MAGIC
# MAGIC - A análise cobre os dados disponíveis de 01/07/2026 a 31/08/2026.
# MAGIC - Os indicadores são descritivos e dependem da qualidade e estrutura dos dados de origem.
# MAGIC - Valores extremos foram preservados e sinalizados, não excluídos automaticamente.
# MAGIC - `ID_SUBCLASSE` apresenta alta incidência de valores nulos na fonte; por isso, o CNPJ da classe/fundo foi utilizado como identificador principal nas análises.
# MAGIC - Os resultados não constituem avaliação regulatória, recomendação de investimento ou conclusão definitiva sobre risco de liquidez de qualquer fundo.