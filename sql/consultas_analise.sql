-- Consultas finais do MVP

-- P1 - maiores taxas acumuladas de resgate sobre PL médio do período.
SELECT
  CNPJ_FUNDO_CLASSE,
  ID_SUBCLASSE,
  SUM(RESG_DIA) AS resgates_periodo,
  AVG(VL_PATRIM_LIQ) AS pl_medio,
  CASE WHEN AVG(VL_PATRIM_LIQ) > 0
       THEN SUM(RESG_DIA) / AVG(VL_PATRIM_LIQ)
       ELSE NULL END AS taxa_resgate_acumulada_rel_pl_medio
FROM workspace.cvm_liquidez.gold_indicadores_liquidez_diarios
WHERE is_valid_base = true
GROUP BY CNPJ_FUNDO_CLASSE, ID_SUBCLASSE
ORDER BY taxa_resgate_acumulada_rel_pl_medio DESC
LIMIT 20;

-- P2 - maior frequência de dias de fluxo líquido negativo.
SELECT
  CNPJ_FUNDO_CLASSE,
  ID_SUBCLASSE,
  COUNT(*) AS dias_validos,
  SUM(CASE WHEN fluxo_liquido < 0 THEN 1 ELSE 0 END) AS dias_fluxo_negativo,
  CASE WHEN COUNT(*) > 0
       THEN SUM(CASE WHEN fluxo_liquido < 0 THEN 1 ELSE 0 END) / COUNT(*)
       ELSE NULL END AS proporcao_dias_fluxo_negativo
FROM workspace.cvm_liquidez.gold_indicadores_liquidez_diarios
WHERE is_valid_base = true
GROUP BY CNPJ_FUNDO_CLASSE, ID_SUBCLASSE
ORDER BY proporcao_dias_fluxo_negativo DESC, dias_fluxo_negativo DESC
LIMIT 20;

-- P3 - mais eventos acima do percentil 95 da amostra.
SELECT
  CNPJ_FUNDO_CLASSE,
  ID_SUBCLASSE,
  SUM(evento_extremo_p95) AS qtd_eventos_extremos_p95,
  COUNT(CASE WHEN taxa_resgate_sobre_pl_anterior IS NOT NULL THEN 1 END) AS dias_com_denominador_d1,
  CASE WHEN COUNT(CASE WHEN taxa_resgate_sobre_pl_anterior IS NOT NULL THEN 1 END) > 0
       THEN SUM(evento_extremo_p95) / COUNT(CASE WHEN taxa_resgate_sobre_pl_anterior IS NOT NULL THEN 1 END)
       ELSE NULL END AS proporcao_eventos_extremos
FROM workspace.cvm_liquidez.gold_indicadores_liquidez_diarios
WHERE is_valid_base = true
GROUP BY CNPJ_FUNDO_CLASSE, ID_SUBCLASSE
ORDER BY qtd_eventos_extremos_p95 DESC, proporcao_eventos_extremos DESC
LIMIT 20;
