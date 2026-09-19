-- Catálogo de Dados do MVP
-- Documenta contexto, campos, tipos/domínios e linhagem das quatro tabelas.

COMMENT ON TABLE workspace.cvm_liquidez.bronze_informe_diario IS 'Camada Bronze: dados do Informe Diário da CVM preservados a partir dos CSVs de origem, com metadados de arquivo e ingestão.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.TP_FUNDO_CLASSE IS 'Tipo do fundo/classe. Origem: CVM. Tipo lógico: string.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.CNPJ_FUNDO_CLASSE IS 'Identificador público da classe/fundo. Origem: CVM. Tipo lógico: string.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.ID_SUBCLASSE IS 'Identificador público da subclasse, quando aplicável. Origem: CVM. Tipo lógico: string ou nulo.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.DT_COMPTC IS 'Data de competência do Informe Diário. Origem: CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.VL_TOTAL IS 'Valor total da carteira na data de competência. Origem: CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.VL_QUOTA IS 'Valor da cota na data de competência. Origem: CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.VL_PATRIM_LIQ IS 'Patrimônio líquido na data de competência. Origem: CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.CAPTC_DIA IS 'Captações realizadas no dia. Origem: CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.RESG_DIA IS 'Resgates do dia. Origem: CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.NR_COTST IS 'Número total de cotistas. Origem: CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario._source_file IS 'Caminho do arquivo de origem obtido por metadado no momento da ingestão.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario._ingestion_ts IS 'Timestamp de ingestão na camada Bronze.';

COMMENT ON TABLE workspace.cvm_liquidez.silver_informe_diario IS 'Camada Silver: dados tipados, padronizados, com chave técnica, flags de qualidade e deduplicação.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.TP_FUNDO_CLASSE IS 'Tipo do fundo/classe. Tipo lógico: string. Linhagem: Bronze.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.CNPJ_FUNDO_CLASSE IS 'Identificador público da classe/fundo. Tipo lógico: string. Linhagem: Bronze.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.ID_SUBCLASSE IS 'Identificador público da subclasse quando disponível. Tipo lógico: string ou nulo. Linhagem: Bronze.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.DT_COMPTC IS 'Data de competência. Tipo lógico: date. Linhagem: Bronze.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.VL_TOTAL IS 'Valor total da carteira. Tipo lógico: double. Linhagem: Bronze.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.VL_QUOTA IS 'Valor da cota. Tipo lógico: double. Linhagem: Bronze.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.VL_PATRIM_LIQ IS 'Patrimônio líquido. Tipo lógico: double. Para registros válidos, PL maior que zero.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.CAPTC_DIA IS 'Captação diária. Tipo lógico: double. Valores negativos foram verificados na etapa de qualidade.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.RESG_DIA IS 'Resgate diário. Tipo lógico: double. Valores negativos foram verificados na etapa de qualidade.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.NR_COTST IS 'Número de cotistas. Tipo lógico: long. Valores negativos foram verificados na etapa de qualidade.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario._source_file IS 'Caminho do arquivo de origem. Linhagem: Bronze.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario._ingestion_ts IS 'Timestamp de ingestão. Linhagem: Bronze.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.ID_SUBCLASSE_CHAVE IS 'Chave técnica: ID_SUBCLASSE quando informado; caso contrário, __SEM_SUBCLASSE__. Tipo lógico: string.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.has_duplicate_key IS 'Flag booleana de duplicidade da chave CNPJ_FUNDO_CLASSE + ID_SUBCLASSE_CHAVE + DT_COMPTC.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.is_valid_base IS 'Flag booleana: CNPJ, data e PL preenchidos e PL maior que zero.';

COMMENT ON TABLE workspace.cvm_liquidez.gold_indicadores_liquidez_diarios IS 'Camada Gold diária: indicadores de fluxo e resgate relativos ao patrimônio líquido e sinalização pelo P95.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.CNPJ_FUNDO_CLASSE IS 'Identificador público da classe/fundo. Tipo lógico: string. Linhagem: Silver.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.TP_FUNDO_CLASSE IS 'Tipo do fundo/classe. Tipo lógico: string. Linhagem: Silver.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.ID_SUBCLASSE IS 'Identificador da subclasse quando disponível. Tipo lógico: string ou nulo. Linhagem: Silver.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.ID_SUBCLASSE_CHAVE IS 'Chave técnica para particionamento da série quando a subclasse está ausente. Tipo lógico: string.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.DT_COMPTC IS 'Data de competência. Tipo lógico: date. Linhagem: Silver.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.VL_PATRIM_LIQ IS 'Patrimônio líquido do dia. Tipo lógico: double. Linhagem: Silver.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.CAPTC_DIA IS 'Captação do dia. Tipo lógico: double. Linhagem: Silver.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.RESG_DIA IS 'Resgate do dia. Tipo lógico: double. Linhagem: Silver.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.NR_COTST IS 'Número de cotistas. Tipo lógico: long. Linhagem: Silver.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.fluxo_liquido IS 'Captações menos resgates do dia. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.taxa_resgate_pl IS 'Resgates do dia / PL do próprio dia, quando PL positivo. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.taxa_fluxo_liquido_pl IS 'Fluxo líquido / PL do próprio dia. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.vl_patrim_liq_d1 IS 'PL do registro anterior da mesma classe/subclasse. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.taxa_resgate_sobre_pl_anterior IS 'Resgates do dia / PL do dia anterior, quando disponível e positivo. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.variacao_pl_d1 IS 'Variação relativa do PL atual contra o PL anterior. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.p95_amostra_taxa_resgate IS 'Percentil 95 da distribuição válida da taxa de resgate sobre PL anterior. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.evento_extremo_p95 IS '1 quando a taxa supera o P95 da amostra; 0 nos demais casos. Tipo lógico: integer.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.is_valid_base IS 'Flag de validade herdada da Silver. Tipo lógico: boolean.';

COMMENT ON TABLE workspace.cvm_liquidez.gold_resumo_liquidez_fundo IS 'Camada Gold resumo: consolidação por CNPJ_FUNDO_CLASSE e ID_SUBCLASSE para P1, P2 e apoio a P3.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.CNPJ_FUNDO_CLASSE IS 'Identificador público da classe/fundo. Tipo lógico: string. Linhagem: Gold diária.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.ID_SUBCLASSE IS 'Identificador da subclasse quando disponível. Tipo lógico: string ou nulo. Linhagem: Gold diária.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.dias_validos IS 'Quantidade de dias válidos. Tipo lógico: long.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.data_inicial IS 'Primeira data válida. Tipo lógico: date.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.data_final IS 'Última data válida. Tipo lógico: date.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.pl_medio IS 'PL médio no período. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.captacoes_periodo IS 'Soma das captações no período. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.resgates_periodo IS 'Soma dos resgates no período. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.fluxo_liquido_periodo IS 'Soma do fluxo líquido no período. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.dias_fluxo_negativo IS 'Quantidade de dias com fluxo líquido negativo. Tipo lógico: long.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.qtd_eventos_extremos_p95 IS 'Quantidade de eventos acima do P95. Tipo lógico: long.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.taxa_resgate_acumulada_rel_pl_medio IS 'Resgates acumulados / PL médio; métrica acumulada, não percentual de um único evento. Tipo lógico: double.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_resumo_liquidez_fundo.proporcao_dias_fluxo_negativo IS 'Proporção de dias válidos com fluxo líquido negativo. Tipo lógico: double.';

-- Linhagem geral: CVM CSV -> Bronze -> Silver -> Gold diária -> Gold resumo.
