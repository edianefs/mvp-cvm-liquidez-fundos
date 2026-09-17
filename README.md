# MVP — Indicadores de Risco de Liquidez em Fundos de Investimento

## Objetivo do projeto

Este MVP demonstra a construção de um pipeline de dados ponta a ponta para apoiar **monitoramento analítico de liquidez em fundos de investimento**, utilizando exclusivamente dados públicos da Comissão de Valores Mobiliários (CVM).

O projeto foi desenhado para um contexto acadêmico de Ciência de Dados e Analytics, com possível aplicação conceitual em **Compliance, Controles Internos e Gestão de Riscos** de uma corretora de valores, sem utilizar dados internos da Banrisul Corretora de Valores Mobiliários e Câmbio e sem utilizar dados pessoais.

> **Importante:** os indicadores deste MVP são métricas analíticas/descritivas para apoio a monitoramento. Eles **não constituem classificação regulatória de risco de liquidez**, nem substituem políticas, metodologias, limites ou controles formais da instituição.

## Fonte de dados

A fonte principal é o conjunto **Fundos de Investimento: Documentos: Informe Diário**, do Portal Dados Abertos da CVM.

A CVM informa que o Informe Diário contém, entre outras, as informações de valor total da carteira, patrimônio líquido, valor da cota, captações, resgates e número de cotistas. O conjunto disponibiliza os informes diários dos fundos dos últimos doze meses e possui atualização periódica. A licença informada no Portal é a **Open Data Commons Open Database License (ODbL)**.

Fonte oficial: https://dados.cvm.gov.br/dataset/fi-doc-inf_diario

No momento de elaboração deste MVP, o portal disponibilizava arquivos mensais e indicava atualização até setembro de 2026. Para evitar dependência de dados incompletos do mês corrente, o pipeline utiliza como amostra principal **julho/2026 e agosto/2026**, dois meses completos imediatamente anteriores ao mês corrente.

Arquivos utilizados pelo pipeline:

- `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202607.zip`
- `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202608.zip`

O projeto não versiona os arquivos de dados no GitHub. Os ZIPs são baixados manualmente da fonte oficial e carregados para o Volume do Databricks antes da execução do notebook.

## Contexto de negócios e perguntas

### Contexto

Em um ambiente de controles internos e gestão de riscos, uma rotina de monitoramento pode precisar identificar, de maneira reproduzível, fundos que apresentem movimentos de resgate relevantes em relação ao patrimônio líquido, sequência de dias de fluxo líquido negativo ou episódios extremos de resgate.

O MVP transforma os registros diários da CVM em indicadores analíticos que podem ser usados como **sinais para investigação**. O objetivo não é afirmar que um fundo possui risco regulatório, mas demonstrar como dados públicos podem sustentar um processo de monitoramento e priorização de exceções.

### Problema

**Como transformar os dados públicos do Informe Diário da CVM em indicadores simples e reproduzíveis que permitam priorizar a análise de eventos de liquidez em fundos de investimento?**

### Perguntas de negócio

**P1.** Quais fundos apresentaram as maiores taxas acumuladas de resgate em relação ao patrimônio líquido no período analisado?

**P2.** Quais fundos apresentaram maior frequência de dias com fluxo líquido negativo (`resgates > captações`)?

**P3.** Quais fundos concentraram mais ocorrências de resgates diários extremos, definidos de forma **relativa à própria amostra** pelo percentil 95 da taxa `resgates / patrimônio líquido do dia anterior`?

Estas perguntas foram mantidas como objetivo original do MVP. Caso alguma não possa ser respondida por problema de qualidade ou disponibilidade dos dados, isso deverá ser explicitado na autoavaliação, conforme orientação do trabalho.

## Estrutura do dado bruto

O Informe Diário da CVM apresenta, no layout atualmente utilizado, os campos principais. A CVM registra que, a partir de 2024, o conjunto passou a utilizar `TP_FUNDO_CLASSE`, `CNPJ_FUNDO_CLASSE` e `ID_SUBCLASSE`.

| Campo | Uso no MVP |
|---|---|
| `TP_FUNDO_CLASSE` | Tipo do registro/fundo, mantido para rastreabilidade |
| `CNPJ_FUNDO_CLASSE` | Identificador público da classe/fundo |
| `ID_SUBCLASSE` | Identificador da subclasse, quando aplicável |
| `DT_COMPTC` | Data de competência do informe |
| `VL_TOTAL` | Valor total da carteira |
| `VL_QUOTA` | Valor da cota |
| `VL_PATRIM_LIQ` | Patrimônio líquido |
| `CAPTC_DIA` | Captações realizadas no dia |
| `RESG_DIA` | Resgates pagos no dia |
| `NR_COTST` | Número de cotistas |

O projeto trabalha somente com informações públicas de fundos. Não são utilizados nomes de clientes, CPF, dados de conta, posição individual de investidores ou informações internas da empresa.

## Carga dos dados (Etapa 4.2)

A preparação dos dados é executada no Notebook `notebooks/01_pipeline_cvm_liquidez.py`. A aquisição dos ZIPs é manual no computador do aluno e a extração/leitura ocorre no Databricks.

Fluxo:

1. Criar um Volume no Unity Catalog para os arquivos brutos.
2. Baixar os dois ZIPs mensais diretamente do Portal Dados Abertos da CVM no computador do aluno.
3. Fazer upload dos ZIPs para `/Volumes/workspace/cvm_liquidez/raw`.
4. O notebook localiza os ZIPs, extrai automaticamente os CSVs e os organiza por mês.
5. Ler os CSVs no Spark e persistir a camada Bronze como tabela Delta.

A carga manual foi adotada porque a Databricks Free Edition restringe o acesso de saída à internet. O trabalho permite explicitamente o fluxo simples de download do dataset e upload para a plataforma em nuvem.
Não é necessário publicar os dados no GitHub; apenas o código é versionado.

## Modelagem e Catálogo de Dados (Etapa 4.3)

Foi adotado um modelo simples, adequado ao escopo do MVP:

- `bronze_informe_diario`: cópia estruturada do dado de origem, preservando o conteúdo original com metadados de ingestão.
- `silver_informe_diario`: dados padronizados, tipados, deduplicados e com regras de qualidade aplicadas.
- `gold_indicadores_liquidez_diarios`: tabela fato analítica diária, com os indicadores calculados para cada fundo/data.
- `gold_resumo_liquidez_fundo`: tabela agregada por fundo para responder diretamente às perguntas de negócio.

### Catálogo de dados

O catálogo completo está no arquivo `sql/catalogo.sql` e também pode ser visualizado no Unity Catalog após as tabelas serem criadas.

### Modelo lógico

`bronze_informe_diario` → `silver_informe_diario` → `gold_indicadores_liquidez_diarios` → `gold_resumo_liquidez_fundo`

Não foi utilizado esquema estrela neste MVP porque o problema possui uma única entidade fato diária e não exige dimensões adicionais para responder às perguntas definidas.

## Pipeline de Dados (Etapa 4.4)

Para simplificar a implementação e facilitar a auditoria acadêmica, o pipeline foi organizado em **um único Notebook Databricks**, com células identificadas por etapa:

1. Configuração e parâmetros.
2. Coleta dos dados CVM.
3. Bronze.
4. Silver.
5. Gold.
6. Qualidade.
7. Consultas finais.

O código está em `notebooks/01_pipeline_cvm_liquidez.py`.

### Comandos que devem aparecer no pipeline

A execução deve evidenciar, no mínimo, os seguintes blocos:

- `CREATE SCHEMA IF NOT EXISTS`
- `CREATE VOLUME IF NOT EXISTS`
- upload dos ZIPs oficiais para o Volume e extração via `zipfile`
- extração via `zipfile`
- `spark.read.option(...).csv(...)`
- `CREATE OR REPLACE TABLE ... USING DELTA`
- `dropDuplicates`
- conversões de tipo com `cast`
- `lag` / janela temporal
- cálculos de `fluxo_liquido`, `taxa_resgate_pl`, `taxa_fluxo_liquido_pl` e `taxa_resgate_sobre_pl_anterior`
- `percentile_approx` para o limite estatístico do percentil 95
- gravação das tabelas Gold

### Execução no Databricks

1. Criar um workspace no Databricks Free Edition.
2. Criar/conectar o repositório GitHub ao Databricks Repos.
3. Importar/conectar o conteúdo deste repositório.
4. No Catalog Explorer, abrir `workspace > cvm_liquidez > Volumes > raw`.
5. Fazer upload dos arquivos `inf_diario_fi_202607.zip` e `inf_diario_fi_202608.zip`.
6. Abrir `notebooks/01_pipeline_cvm_liquidez.py` como notebook.
7. Executar as células em ordem, do início ao fim.
8. Verificar as tabelas no catálogo.
9. Executar `02_qualidade_cvm_liquidez.py` e depois `03_analise_cvm_liquidez.py`.

## Qualidade de Dados (Etapa 4.5)

A qualidade é tratada antes da análise final.

São verificados:

- **Completude:** nulos em identificador, data e métricas principais.
- **Consistência:** tipos de data e numéricos; valores negativos em métricas que, no contexto do dado, deveriam ser não negativos.
- **Unicidade:** duplicidade por `CNPJ_FUNDO_CLASSE + ID_SUBCLASSE + DT_COMPTC`, usando uma chave técnica para subclasse ausente.
- **Acurácia lógica:** casos de patrimônio líquido zero/nulo, necessários para evitar divisão por zero.
- **Outliers:** distribuição das taxas de resgate e fluxo líquido, sem exclusão automática de extremos.

A regra central do MVP é **não apagar silenciosamente eventos extremos**. Os registros permanecem disponíveis e são sinalizados para análise.

As consultas de qualidade estão em `notebooks/02_qualidade_cvm_liquidez.py`.

## Análise de Dados (Etapa 4.5)

A análise final está em `notebooks/03_analise_cvm_liquidez.py` e pode ser executada após o pipeline principal.

### Indicadores

**Fluxo líquido diário**

`fluxo_liquido = CAPTC_DIA - RESG_DIA`

**Taxa diária de resgate sobre patrimônio líquido**

`taxa_resgate_pl = RESG_DIA / VL_PATRIM_LIQ`

**Taxa diária de fluxo líquido sobre patrimônio líquido**

`taxa_fluxo_liquido_pl = (CAPTC_DIA - RESG_DIA) / VL_PATRIM_LIQ`

**Taxa de resgate sobre patrimônio líquido do dia anterior**

`taxa_resgate_sobre_pl_anterior = RESG_DIA / VL_PATRIM_LIQ_D1`

A utilização do patrimônio líquido do dia anterior busca evitar que o próprio resgate do dia seja usado simultaneamente como denominador do indicador de estresse.

**Evento extremo relativo da amostra**

É calculado o percentil 95 (`P95`) da `taxa_resgate_sobre_pl_anterior` na amostra válida. Um evento acima desse percentil é marcado como `evento_extremo_p95 = 1`.

Isso é um critério estatístico do MVP, e **não um limite regulatório da CVM**.

### Respostas das perguntas

O notebook final produz três tabelas de resposta:

- `resultado_p1_maiores_taxas_resgate`
- `resultado_p2_maior_frequencia_fluxo_negativo`
- `resultado_p3_eventos_extremos_p95`

Os valores apresentados no trabalho devem ser copiados **após a execução real** no Databricks. Não devem ser preenchidos com números estimados.

### Texto pronto para P1 — substituir os campos entre colchetes

> **P1 — Resultado:** No período analisado, os fundos que apresentaram as maiores taxas acumuladas de resgate relativas ao patrimônio líquido médio foram **[CNPJ 1]**, **[CNPJ 2]** e **[CNPJ 3]**, conforme a ordenação produzida pela tabela `resultado_p1_maiores_taxas_resgate`. As taxas observadas foram, respectivamente, **[valor 1]**, **[valor 2]** e **[valor 3]**. O resultado indica maior intensidade relativa de resgates na amostra e serve como sinal para priorização de análise.

### Texto pronto para P2 — substituir os campos entre colchetes

> **P2 — Resultado:** Os fundos com maior frequência de dias de fluxo líquido negativo foram **[CNPJ 1]**, **[CNPJ 2]** e **[CNPJ 3]**, com **[n1]**, **[n2]** e **[n3]** dias negativos, correspondendo a **[p1]%**, **[p2]%** e **[p3]%** dos dias válidos, respectivamente. A métrica indica recorrência de saídas líquidas na amostra, não uma classificação regulatória de risco.

### Texto pronto para P3 — substituir os campos entre colchetes

> **P3 — Resultado:** O percentil 95 da taxa diária de resgate sobre o patrimônio líquido do dia anterior foi de **[P95 real]**. Os fundos com maior quantidade de ocorrências acima desse ponto foram **[CNPJ 1]**, **[CNPJ 2]** e **[CNPJ 3]**, com **[n1]**, **[n2]** e **[n3]** ocorrências. O P95 foi utilizado exclusivamente como critério estatístico relativo à amostra do MVP e não representa limite ou parâmetro regulatório da CVM.

## Evidências / screenshots exigidos

Inserir no README/PDF final as seguintes imagens, nesta ordem:

**Imagem 1 — Fonte dos dados.** Screenshot da página da CVM mostrando o conjunto “Fundos de Investimento: Documentos: Informe Diário” e a licença.

**Imagem 2 — Arquivos baixados.** Screenshot do volume/caminho no Databricks contendo os arquivos de julho e agosto de 2026.

**Imagem 3 — Bronze.** Screenshot da tabela `bronze_informe_diario` no Databricks, mostrando colunas e registros.

**Imagem 4 — Catálogo.** Screenshot do Unity Catalog com a tabela Silver e suas descrições de campos.

**Imagem 5 — Pipeline.** Screenshot do Notebook `01_pipeline_cvm_liquidez.py` mostrando os blocos de execução Bronze → Silver → Gold.

**Imagem 6 — Persistência.** Screenshot mostrando as tabelas Gold criadas no Databricks.

**Imagem 7 — Qualidade.** Screenshot do resultado das consultas de qualidade.

**Imagem 8 — Resultado P1.** Screenshot do resultado da consulta que responde P1.

**Imagem 9 — Resultado P2.** Screenshot do resultado da consulta que responde P2.

**Imagem 10 — Resultado P3.** Screenshot do resultado da consulta que responde P3.

Não inserir screenshots de dados internos da Banrisul Corretora, clientes ou qualquer informação não pública.

## GitHub

O repositório deve ser público e conter pelo menos:

```text
mvp_cvm_liquidez/
├── README.md
├── notebooks/
│   ├── 01_pipeline_cvm_liquidez.py
│   ├── 02_qualidade_cvm_liquidez.py
│   └── 03_analise_cvm_liquidez.py
├── sql/
│   ├── catalogo.sql
│   └── consultas_analise.sql
└── docs/
    └── ENTENDIMENTO_NAO_ENTRA_NO_TRABALHO.md
```

## Autoavaliação

### Atingimento do objetivo

> **Preencher após a execução:** O objetivo do MVP foi **[atingido integralmente / atingido parcialmente]**. A pergunta P1 foi **[respondida / não respondida]**, a P2 foi **[respondida / não respondida]** e a P3 foi **[respondida / não respondida]**. As principais razões para eventuais limitações foram **[descrever somente fatos que realmente ocorreram]**.

### Dificuldades encontradas

> **Preencher após a execução:** Durante a implementação, as principais dificuldades foram **[descrever apenas as dificuldades efetivamente encontradas]**. As soluções adotadas foram **[descrever as soluções efetivamente aplicadas]**.

### Trabalhos futuros

Como evolução do MVP, podem ser considerados:

- inclusão de janela histórica maior;
- inclusão do cadastro de fundos para trazer atributos cadastrais e nomes, quando apropriado;
- acompanhamento automatizado e periódico;
- dashboard de exceções;
- definição institucional de limites, somente com metodologia aprovada pela área responsável;
- monitoramento de reincidência de eventos e tempo de recuperação.

## Referências

- CVM — Fundos de Investimento: Documentos: Informe Diário: https://dados.cvm.gov.br/dataset/fi-doc-inf_diario
- CVM — Fundos de Investimento: Informação Cadastral: https://dados.cvm.gov.br/dataset/fi-cad
- Databricks — Documentação: https://docs.databricks.com/
- Databricks — Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- Databricks — Delta Lake: https://docs.databricks.com/aws/en/delta/

