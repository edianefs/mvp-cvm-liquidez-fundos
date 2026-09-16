-- Catálogo de Dados do MVP
-- Execute depois que as tabelas forem criadas pelo notebook principal.

CREATE TABLE IF NOT EXISTS workspace.cvm_liquidez.bronze_informe_diario
COMMENT 'Dados do Informe Diário de Fundos de Investimento da CVM, armazenados na forma estruturada de entrada com metadados de ingestão.';

COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.TP_FUNDO IS 'Tipo de fundo informado pela fonte CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.CNPJ_FUNDO IS 'Identificador público do fundo informado pela CVM.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.DT_COMPTC IS 'Data de competência do Informe Diário.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.VL_TOTAL IS 'Valor total da carteira do fundo na data de competência.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.VL_QUOTA IS 'Valor da cota na data de competência.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.VL_PATRIM_LIQ IS 'Patrimônio líquido do fundo na data de competência.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.CAPTC_DIA IS 'Captações realizadas no dia.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.RESG_DIA IS 'Resgates pagos no dia.';
COMMENT ON COLUMN workspace.cvm_liquidez.bronze_informe_diario.NR_COTST IS 'Número de cotistas.';

ALTER TABLE workspace.cvm_liquidez.silver_informe_diario
SET TBLPROPERTIES ('quality.description' = 'Dados tipados, padronizados e deduplicados do Informe Diário.');

COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.is_valid_base IS 'Indica se o registro possui identificador, data e patrimônio líquido válidos para os indicadores principais.';
COMMENT ON COLUMN workspace.cvm_liquidez.silver_informe_diario.has_duplicate_key IS 'Indica se a chave CNPJ_FUNDO + DT_COMPTC foi identificada como duplicada antes da deduplicação.';

ALTER TABLE workspace.cvm_liquidez.gold_indicadores_liquidez_diarios
SET TBLPROPERTIES ('quality.description' = 'Indicadores diários analíticos de fluxo e resgate relativos ao patrimônio líquido.');

COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.fluxo_liquido IS 'Captações do dia menos resgates do dia.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.taxa_resgate_pl IS 'Resgates do dia divididos pelo patrimônio líquido do próprio dia, quando o denominador é positivo.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.taxa_fluxo_liquido_pl IS 'Fluxo líquido do dia dividido pelo patrimônio líquido do próprio dia.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.vl_patrim_liq_d1 IS 'Patrimônio líquido do fundo no registro anterior conforme ordenação temporal.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.taxa_resgate_sobre_pl_anterior IS 'Resgates do dia divididos pelo patrimônio líquido do dia anterior, quando disponível e positivo.';
COMMENT ON COLUMN workspace.cvm_liquidez.gold_indicadores_liquidez_diarios.evento_extremo_p95 IS 'Indicador estatístico: 1 quando a taxa de resgate sobre PL anterior supera o percentil 95 da amostra válida.';
