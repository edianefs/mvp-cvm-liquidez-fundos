# MVP — Indicadores de Risco de Liquidez em Fundos de Investimento

## 1. Objetivo

Este MVP demonstra a construção de um pipeline de dados ponta a ponta para apoiar o **monitoramento analítico de liquidez em fundos de investimento**, utilizando dados públicos da Comissão de Valores Mobiliários (CVM).

O projeto foi desenvolvido em Databricks, com processamento em PySpark e armazenamento em tabelas Delta no Unity Catalog. O código é versionado no GitHub.

Os indicadores produzidos são **descritivos e analíticos**. Eles servem como sinais para investigação e monitoramento e não constituem classificação regulatória de risco, recomendação de investimento ou substituição de metodologias e controles institucionais.

## 2. Fonte dos dados

A fonte principal é o conjunto **Fundos de Investimento: Documentos: Informe Diário**, do Portal Dados Abertos da CVM:

https://dados.cvm.gov.br/dataset/fi-doc-inf_diario

O Informe Diário contém, entre outras informações, patrimônio líquido, valor da cota, captações, resgates e número de cotistas.

A amostra utilizada no MVP compreende os meses completos de **julho/2026 e agosto/2026**:

- `inf_diario_fi_202607.zip`
- `inf_diario_fi_202608.zip`

Os arquivos de origem não são versionados no GitHub. O código do pipeline e as evidências da execução são versionados.
### Licença e contexto da fonte

O conjunto **Fundos de Investimento: Documentos: Informe Diário** está disponibilizado no Portal Dados Abertos da CVM sob a **Licença Aberta para Bases de Dados (ODbL) do Open Data Commons**. A página da CVM também disponibiliza o dicionário de dados do conjunto e informa que os dados são disponibilizados em CSV compactado (ZIP). citeturn0search0turn0search1


## 3. Contexto e perguntas de negócio

O MVP busca transformar os registros diários da CVM em indicadores reproduzíveis para apoiar a identificação de eventos que mereçam investigação no contexto de monitoramento de liquidez.

### Problema

**Como transformar os dados públicos do Informe Diário da CVM em indicadores simples e reproduzíveis que permitam priorizar a análise de eventos de liquidez em fundos de investimento?**

### Perguntas

**P1.** Quais fundos apresentaram as maiores taxas acumuladas de resgate em relação ao patrimônio líquido médio no período?

**P2.** Quais fundos apresentaram maior frequência de dias com fluxo líquido negativo?

**P3.** Quais fundos concentraram mais ocorrências de resgates diários extremos, definidos relativamente à amostra pelo percentil 95 da taxa de resgate sobre o patrimônio líquido do dia anterior?


## 4. Carga dos dados

A carga foi realizada no **Databricks Free Edition**, utilizando um Volume do Unity Catalog como área de armazenamento dos arquivos de origem.

O fluxo foi: (1) disponibilização dos arquivos mensais no Volume `workspace.cvm_liquidez.raw`; (2) verificação dos ZIPs pelo notebook principal; (3) extração dos CSVs por mês no próprio Volume; (4) leitura dos CSVs pelo Spark com cabeçalho e separador `;`; e (5) gravação da camada Bronze em formato Delta.

O script responsável por essa etapa é `notebooks/01_pipeline_cvm_liquidez.py`. A partir da Bronze, o mesmo notebook executa as transformações para Silver e Gold. A CVM informa que o Informe Diário é disponibilizado em CSV compactado (ZIP). citeturn0search0turn0search11

## 5. Modelagem e catálogo de dados

A modelagem segue a lógica de arquitetura medalhão:

| Camada | Tabela | Finalidade |
|---|---|---|
| Bronze | `workspace.cvm_liquidez.bronze_informe_diario` | Preservar os dados de origem com metadados de ingestão |
| Silver | `workspace.cvm_liquidez.silver_informe_diario` | Tipar, padronizar, sinalizar duplicidades, deduplicar e validar |
| Gold diária | `workspace.cvm_liquidez.gold_indicadores_liquidez_diarios` | Produzir indicadores diários e eventos extremos |
| Gold resumo | `workspace.cvm_liquidez.gold_resumo_liquidez_fundo` | Consolidar indicadores por fundo/classe e período |

O catálogo de dados está implementado em `sql/catalogo.sql` e complementado pelo screenshot do Unity Catalog em `docs/imagens/evidencias%20do%20catalogo%20-%20modelagem%20e%20catalogo%20de%20dados.PNG`.

O dicionário da fonte contempla informações como tipo de fundo/classe, identificador, subclasse, data de competência, valor total da carteira, patrimônio líquido, valor da cota, captações, resgates e número de cotistas. citeturn0search0

A linhagem está documentada no catálogo: Bronze recebe os CSVs da CVM; Silver deriva da Bronze por tipagem, chave técnica, deduplicação e validação; Gold diária deriva da Silver por cálculos de indicadores; Gold resumo agrega a Gold diária por fundo/classe. Os comentários do `sql/catalogo.sql` registram descrição, tipo lógico, domínio quando aplicável e origem dos campos.

## 6. Arquitetura do pipeline

O fluxo implementado é:

`dados públicos CVM → Bronze → Silver → Gold diária → Gold resumo → análise`

### Bronze

`bronze_informe_diario`

Preserva os dados de origem em formato Delta e acrescenta metadados de ingestão e arquivo de origem.

### Silver

`silver_informe_diario`

Realiza:

- padronização dos tipos;
- conversão da data de competência;
- tratamento técnico de `ID_SUBCLASSE` ausente;
- identificação de chaves duplicadas;
- deduplicação;
- criação da variável `is_valid_base`.

### Gold diária

`gold_indicadores_liquidez_diarios`

Calcula os indicadores diários, incluindo fluxo líquido, taxas de resgate e fluxo líquido sobre patrimônio líquido, patrimônio líquido do dia anterior e sinalização de eventos acima do P95 da amostra.

### Gold resumo

`gold_resumo_liquidez_fundo`

Agrega os dados por `CNPJ_FUNDO_CLASSE` e `ID_SUBCLASSE`, permitindo responder diretamente às perguntas P1 e P2 e apoiar a análise P3.

## 7. Indicadores

### Fluxo líquido diário

`fluxo_liquido = CAPTC_DIA - RESG_DIA`

### Taxa diária de resgate sobre PL

`taxa_resgate_pl = RESG_DIA / VL_PATRIM_LIQ`

### Taxa diária de fluxo líquido sobre PL

`taxa_fluxo_liquido_pl = (CAPTC_DIA - RESG_DIA) / VL_PATRIM_LIQ`

### Taxa de resgate sobre PL do dia anterior

`taxa_resgate_sobre_pl_anterior = RESG_DIA / VL_PATRIM_LIQ_D1`

O uso do PL do dia anterior permite analisar o volume de resgate em relação à base patrimonial observada antes do evento.

### Evento extremo pelo P95

O percentil 95 da distribuição amostral de `taxa_resgate_sobre_pl_anterior` é calculado sobre os registros válidos. Observações acima desse ponto recebem `evento_extremo_p95 = 1`.

O P95 é um **critério estatístico construído para este MVP** e não representa limite, regra ou parâmetro regulatório da CVM.

### Indicador acumulado de P1

`taxa_resgate_acumulada_rel_pl_medio`

representa a razão entre os resgates acumulados no período e o PL médio observado. Por ser acumulado, não deve ser interpretado como percentual do patrimônio resgatado em um único evento.

## 8. Qualidade dos dados

A qualidade foi verificada antes da análise final, contemplando:

- completude;
- consistência;
- duplicidades;
- registros não válidos para os indicadores principais;
- valores extremos.

### Resultados observados

- Bronze: **1.119.386 linhas**
- Silver: **1.119.383 linhas**
- Gold diária: **1.119.383 linhas**
- Gold resumo: **26.077 linhas**
- Período analisado: **01/07/2026 a 31/08/2026**
- Registros não válidos para os indicadores principais: **4.720**
- Chaves duplicadas identificadas no Bronze: **3**
- Eventos acima do P95: **54.476**

A diferença de três linhas entre Bronze e Silver corresponde às duplicidades removidas pela regra de deduplicação.

`ID_SUBCLASSE` apresenta alta incidência de valores nulos na fonte. Esses valores não foram artificialmente preenchidos. Foi criada uma chave técnica para permitir o tratamento consistente das observações sem subclasse, mantendo o CNPJ da classe/fundo como principal identificador público das análises.

Valores extremos foram preservados e sinalizados, em vez de serem excluídos automaticamente.

## 9. Resultados analíticos

### P1 — Resgates acumulados sobre PL médio

O indicador identifica fundos com maior volume de resgates acumulados em relação ao PL médio do período.

O maior valor observado na amostra foi de aproximadamente **46,31**, referente ao CNPJ `52.984.696/0001-31`. Esse resultado deve ser interpretado como uma razão acumulada entre resgates e PL médio, e não como a afirmação de que o fundo resgatou 46 vezes seu patrimônio em um único evento.

### P2 — Frequência de fluxo líquido negativo

Os resultados identificam fundos com recorrência de dias em que:

`CAPTC_DIA - RESG_DIA < 0`

Na primeira posição da tabela, há fundos com **44 dias válidos e 44 dias de fluxo líquido negativo**, correspondendo a 100% dos dias considerados para aquele fundo. A métrica descreve recorrência de saída líquida no período e, isoladamente, não caracteriza situação regulatória.

### P3 — Eventos acima do P95

O P95 calculado na amostra foi aproximadamente **0,43%**.

Os resultados de P3 identificam fundos que apresentaram maior quantidade de observações acima desse ponto estatístico. Trata-se de uma classificação relativa à distribuição observada nos dados do MVP, sem interpretação como limite regulatório.

## 10. Exemplo de série temporal

Foi analisada a série do CNPJ `52.984.696/0001-31` para demonstrar o comportamento dos indicadores ao longo do período.

Em 06/07/2026, por exemplo, foi observado resgate de aproximadamente R$ 26,86 milhões frente a PL anterior de aproximadamente R$ 579,4 mil, resultando em taxa de resgate sobre PL anterior próxima de 46,34.

O exemplo evidencia por que eventos extremos devem ser preservados e investigados, em vez de removidos automaticamente.

## 11. Tratamento de erros e decisões técnicas

Durante a execução foram registrados erros e as respectivas correções. As evidências foram preservadas no repositório, conforme a orientação acadêmica de documentar o processo de resolução.

### Erro 1 — referência de coluna inexistente

**Problema identificado:** uma etapa do pipeline apresentou erro de resolução de coluna relacionado ao campo `TP_FUNDO`.

**Diagnóstico:** o layout utilizado pela CVM emprega `TP_FUNDO_CLASSE`, e não `TP_FUNDO`.

**Ação corretiva:** a referência foi ajustada para `TP_FUNDO_CLASSE`, mantendo o campo compatível com o layout efetivamente carregado.

**Resultado:** a etapa passou a executar corretamente e a tabela Silver foi gravada.

**Evidências:**

![Erro de referência de coluna](docs/imagens/primeiro%20erro%20-.PNG)

![Correção e resultado](docs/imagens/p1c3%20-%20altera%C3%A7%C3%A3o%20da%20celula%20com%20erro%20e%20resultado.PNG)

### Outras verificações de qualidade

Além do erro de implementação registrado acima, foram verificadas duplicidades, registros inválidos, valores negativos e outliers. Essas ocorrências foram tratadas como parte da qualidade dos dados e não como erros de execução.

As evidências completas permanecem em `docs/imagens/`.

## 12. Evidências da execução

As principais evidências visuais estão organizadas em `docs/imagens/`.

### Fonte e carga

![Dados públicos da CVM](docs/imagens/dados%20publicos%20carregados%20para%20a%20nuvem%20-%20carga%20de%20dados.PNG)

### Modelagem e catálogo

![Catálogo de dados](docs/imagens/evidencias%20do%20catalogo%20-%20modelagem%20e%20catalogo%20de%20dados.PNG)

### Camada Gold

![Camada Gold](docs/imagens/camada%20gold.PNG)

### Qualidade

![Completude da Silver](docs/imagens/p2c12_%20completude%20silver.PNG)

### P1

![Resultado P1](docs/imagens/p3c1%20-%20p1.PNG)

### P2

![Resultado P2](docs/imagens/p3c2%20-%20p2.PNG)

### P3

![Resultado P3](docs/imagens/p3c3%20-%20p3.PNG)

As demais evidências de execução, inclusive as utilizadas na validação das camadas Bronze, Silver e Gold, permanecem no diretório `docs/imagens/`.

## 13. Estrutura do repositório

```
mvp-cvm-liquidez-fundos/
├── README.md
├── notebooks/
│   ├── 01_pipeline_cvm_liquidez.py
│   ├── 02_qualidade_cvm_liquidez.py
│   └── 03_analise_cvm_liquidez.py
├── sql/
│   ├── catalogo.sql
│   └── consultas_analise.sql
└── docs/
    └── imagens/
```

## 14. Reprodutibilidade

O pipeline foi implementado no Databricks e organizado em três notebooks:

1. `01_pipeline_cvm_liquidez.py` — ingestão, Bronze, Silver e Gold;
2. `02_qualidade_cvm_liquidez.py` — verificações de qualidade;
3. `03_analise_cvm_liquidez.py` — respostas às perguntas P1, P2 e P3 e exemplo de série temporal.

As tabelas utilizadas são:

- `workspace.cvm_liquidez.bronze_informe_diario`
- `workspace.cvm_liquidez.silver_informe_diario`
- `workspace.cvm_liquidez.gold_indicadores_liquidez_diarios`
- `workspace.cvm_liquidez.gold_resumo_liquidez_fundo`

## 15. Limitações

- A análise cobre somente 01/07/2026 a 31/08/2026.
- Os indicadores dependem da qualidade e da estrutura dos dados públicos de origem.
- O indicador de resgates acumulados sobre PL médio é uma razão acumulada e deve ser interpretado nesse contexto.
- O P95 é um limiar estatístico da amostra do MVP, não um parâmetro regulatório.
- Valores extremos foram preservados e sinalizados.
- A alta incidência de `ID_SUBCLASSE` nulo limita análises específicas por subclasse.
- Os resultados não constituem avaliação regulatória, recomendação de investimento ou conclusão definitiva sobre risco de liquidez de qualquer fundo.

## 16. Trabalhos futuros

Como evolução do MVP, podem ser considerados:

- ampliação da janela histórica;
- integração com dados cadastrais dos fundos;
- atualização periódica automatizada;
- dashboard de acompanhamento de exceções;
- análise de reincidência e persistência dos eventos;
- definição de indicadores complementares de liquidez.


## 17. Autoavaliação

O objetivo definido no início do MVP foi atingido no escopo proposto. Foi construído um pipeline de dados ponta a ponta no Databricks, partindo de dados públicos da CVM, passando pelas camadas Bronze, Silver e Gold e chegando a consultas analíticas que respondem às três perguntas de negócio.

As três perguntas foram respondidas com indicadores reproduzíveis: P1 analisou resgates acumulados em relação ao PL médio, P2 analisou a frequência de dias com fluxo líquido negativo e P3 identificou ocorrências acima do P95 da amostra. As respostas foram acompanhadas de interpretação e evidências visuais no repositório.

As principais dificuldades estiveram no entendimento do layout da fonte, no tratamento de nulos e duplicidades e na interpretação adequada de eventos extremos. Durante a execução, o erro relacionado à coluna `TP_FUNDO` foi identificado e corrigido para `TP_FUNDO_CLASSE`, com as evidências preservadas. A alta incidência de `ID_SUBCLASSE` nulo também exigiu uma decisão técnica para manter a rastreabilidade sem preenchimento artificial.

O MVP demonstrou o ciclo completo de coleta, armazenamento, transformação, validação e análise na nuvem. Como evolução, podem ser ampliados o histórico, a automação de atualização, a integração com dados cadastrais e o acompanhamento contínuo das exceções.

## 18. Referências

- CVM — Fundos de Investimento: Documentos: Informe Diário: https://dados.cvm.gov.br/dataset/fi-doc-inf_diario
- CVM — Fundos de Investimento: Informação Cadastral: https://dados.cvm.gov.br/dataset/fi-cad
- Databricks — Documentação: https://docs.databricks.com/
- Databricks — Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- Databricks — Delta Lake: https://docs.databricks.com/aws/en/delta/
