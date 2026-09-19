# MVP — Indicadores de Risco de Liquidez em Fundos de Investimento

## 1. Objetivo

Este MVP mostra, na prática, como usei dados públicos da Comissão de Valores Mobiliários (CVM) para fazer uma análise sobre liquidez em fundos de investimento.

O projeto foi desenvolvido no Databricks, usando os recursos disponíveis na plataforma para carregar, organizar e analisar os dados. O código utilizado está no GitHub.

Os indicadores apresentados servem para observar os dados e encontrar situações que mereçam uma análise mais detalhada. Eles não representam uma classificação regulatória de risco nem uma recomendação de investimento.

## 2. Fonte dos dados

A fonte principal é o conjunto Fundos de Investimento: Documentos: Informe Diário, do Portal Dados Abertos da CVM:

https://dados.cvm.gov.br/dataset/fi-doc-inf_diario

O Informe Diário contém, entre outras informações, patrimônio líquido, valor da cota, captações, resgates e número de cotistas.

A estrutura dos dados brutos utilizada neste MVP é composta por registros diários de fundos/classes, com os principais campos abaixo:

| Campo bruto | Conteúdo |
|---|---|
| `TP_FUNDO_CLASSE` | Tipo do fundo/classe |
| `CNPJ_FUNDO_CLASSE` | Identificador público do fundo/classe |
| `ID_SUBCLASSE` | Identificador da subclasse, quando informado |
| `DT_COMPTC` | Data de competência |
| `VL_TOTAL` | Valor total da carteira |
| `VL_QUOTA` | Valor da cota |
| `VL_PATRIM_LIQ` | Patrimônio líquido |
| `CAPTC_DIA` | Captação do dia |
| `RESG_DIA` | Resgate do dia |
| `NR_COTST` | Número de cotistas |

Os arquivos brutos utilizados são os dois ZIPs mensais informados acima. Cada arquivo contém registros diários de fundos/classes para a competência correspondente. No pipeline, esses registros são carregados inicialmente em uma única tabela Bronze, a partir da qual são criadas as tabelas Silver e Gold.

A amostra utilizada no MVP compreende os meses completos de julho/2026 e agosto/2026:

- `inf_diario_fi_202607.zip`
- `inf_diario_fi_202608.zip`

Os arquivos de origem não são versionados no GitHub. O código do pipeline e as evidências da execução são versionados.
### Licença e contexto da fonte

O conjunto Fundos de Investimento: Documentos: Informe Diário está disponibilizado no Portal Dados Abertos da CVM sob a Licença Aberta para Bases de Dados (ODbL) do Open Data Commons. A página da CVM também disponibiliza o dicionário de dados do conjunto e informa que os dados são disponibilizados em CSV compactado (ZIP).


## 3. Contexto de Negócios e Perguntas

Neste MVP, busco transformar os registros diários da CVM em informações que ajudem a observar o comportamento de liquidez dos fundos de investimento, pensando em uma situação próxima da realidade de uma corretora de valores.

### Problema

Como usar os dados públicos do Informe Diário da CVM para encontrar situações de liquidez que mereçam ser observadas com mais atenção?

### Perguntas

**P1.** Quais fundos apresentaram as maiores taxas acumuladas de resgate em relação ao patrimônio líquido médio no período?

**P2.** Quais fundos apresentaram maior frequência de dias com fluxo líquido negativo?

**P3.** Quais fundos concentraram mais ocorrências de resgates diários extremos, definidos relativamente à amostra pelo percentil 95 da taxa de resgate sobre o patrimônio líquido do dia anterior?


## 4. Carga dos dados

A carga dos dados foi feita no Databricks Free Edition. Os arquivos foram colocados em uma área de armazenamento da plataforma antes de serem lidos e organizados.

O fluxo foi:

1. Disponibilização dos arquivos mensais no Volume `workspace.cvm_liquidez.raw`.
2. Verificação dos ZIPs pelo notebook principal.
3. Extração dos CSVs por mês no próprio Volume.
4. Leitura dos CSVs pelo Spark, utilizando cabeçalho e separador `;`.
5. Gravação da camada Bronze em formato Delta.

O script responsável por essa etapa é `notebooks/01_pipeline_cvm_liquidez.py`. A partir da Bronze, o mesmo notebook executa as transformações para Silver e Gold. A CVM informa que o Informe Diário é disponibilizado em CSV compactado (ZIP).

## 5. Modelagem e catálogo de dados

Para organizar o projeto, os dados foram separados em etapas, seguindo a estrutura Bronze, Silver e Gold:

| Camada | Tabela | Finalidade |
|---|---|---|
| Bronze | `workspace.cvm_liquidez.bronze_informe_diario` | Guardar os dados recebidos da fonte e algumas informações sobre sua origem |
| Silver | `workspace.cvm_liquidez.silver_informe_diario` | Organizar os tipos dos campos, verificar problemas, tratar duplicidades e validar os dados |
| Gold diária | `workspace.cvm_liquidez.gold_indicadores_liquidez_diarios` | Calcular os indicadores diários e identificar valores acima do P95 |
| Gold resumo | `workspace.cvm_liquidez.gold_resumo_liquidez_fundo` | Juntar os resultados por fundo/classe e período |

### Catálogo de dados

O catálogo explicita o domínio de valores de cada campo. Quando a fonte não define um limite máximo, é registrada a regra de negócio aplicável em vez de inventar um limite numérico.

#### Bronze — dados recebidos

| Campo | Tipo lógico | Domínio / regra | Descrição / origem |
|---|---|---|---|
| `TP_FUNDO_CLASSE` | string | Categorias de tipo de fundo/classe conforme a CVM | Tipo do fundo/classe; origem CVM |
| `CNPJ_FUNDO_CLASSE` | string | Identificador público; preenchimento esperado na base válida | Identificador público da classe/fundo; origem CVM |
| `ID_SUBCLASSE` | string / nulo | Identificador da subclasse ou nulo quando não informado | Identificador da subclasse; origem CVM |
| `DT_COMPTC` | date | Data; no MVP, 01/07/2026 a 31/08/2026 | Data de competência; origem CVM |
| `VL_TOTAL` | double | Valor monetário numérico; sem limite superior definido pela fonte | Valor total da carteira; origem CVM |
| `VL_QUOTA` | double | Valor monetário numérico; sem limite superior definido pela fonte | Valor da cota; origem CVM |
| `VL_PATRIM_LIQ` | double | Numérico; pode conter negativos na origem; para os principais indicadores, deve ser > 0 | Patrimônio líquido; origem CVM |
| `CAPTC_DIA` | double | Valor monetário numérico; não foram encontrados negativos na amostra | Captação do dia; origem CVM |
| `RESG_DIA` | double | Valor monetário numérico; não foram encontrados negativos na amostra | Resgate do dia; origem CVM |
| `NR_COTST` | long | Contagem inteira; esperado >= 0 | Número de cotistas; origem CVM |
| `_source_file` | string | Texto com o arquivo de origem | Arquivo identificado na ingestão |
| `_ingestion_ts` | timestamp | Timestamp gerado na ingestão | Momento da ingestão |

#### Silver — dados preparados

A Silver mantém os campos da Bronze utilizados no projeto e acrescenta campos para controle de qualidade.

| Campo | Tipo lógico | Domínio / regra | Descrição / regra |
|---|---|---|---|
| `TP_FUNDO_CLASSE` | string | Categorias de tipo de fundo/classe conforme a CVM | Tipo do fundo/classe |
| `CNPJ_FUNDO_CLASSE` | string | Identificador público; obrigatório para `is_valid_base` | Identificador público |
| `ID_SUBCLASSE` | string / nulo | String ou nulo | Identificador da subclasse |
| `DT_COMPTC` | date | Data; obrigatória para `is_valid_base` | Data de competência |
| `VL_TOTAL` | double | Valor monetário numérico; sem limite superior definido pela fonte | Valor total da carteira |
| `VL_QUOTA` | double | Valor monetário numérico; sem limite superior definido pela fonte | Valor da cota |
| `VL_PATRIM_LIQ` | double | `is_valid_base` exige valor > 0; negativos são preservados | Patrimônio líquido |
| `CAPTC_DIA` | double | Numérico; negativos não observados na amostra | Captação diária |
| `RESG_DIA` | double | Numérico; negativos não observados na amostra | Resgate diário |
| `NR_COTST` | long | Inteiro >= 0 esperado | Número de cotistas |
| `_source_file` | string | Texto com arquivo de origem | Arquivo de origem |
| `_ingestion_ts` | timestamp | Timestamp | Momento da ingestão |
| `ID_SUBCLASSE_CHAVE` | string | ID da subclasse ou `__SEM_SUBCLASSE__` | Chave técnica |
| `has_duplicate_key` | boolean | true/false | Indica duplicidade |
| `is_valid_base` | boolean | true/false; CNPJ e data preenchidos e PL > 0 | Indica validade para os cálculos |

#### Gold diária — indicadores

| Campo | Tipo lógico | Domínio / regra | Descrição / origem |
|---|---|---|---|
| `CNPJ_FUNDO_CLASSE` | string | Identificador público | Silver |
| `TP_FUNDO_CLASSE` | string | Categorias de tipo de fundo/classe conforme a CVM | Silver |
| `ID_SUBCLASSE` | string / nulo | String ou nulo | Silver |
| `ID_SUBCLASSE_CHAVE` | string | Chave técnica da série | Silver |
| `DT_COMPTC` | date | Data do período analisado | Silver |
| `VL_PATRIM_LIQ` | double | Numérico; cálculos principais usam base válida | PL do dia; Silver |
| `CAPTC_DIA` | double | Numérico | Captação do dia; Silver |
| `RESG_DIA` | double | Numérico | Resgate do dia; Silver |
| `NR_COTST` | long | Inteiro >= 0 esperado | Número de cotistas; Silver |
| `fluxo_liquido` | double | Pode ser negativo, zero ou positivo | Captações menos resgates |
| `taxa_resgate_pl` | double | >= 0 quando calculada com PL positivo | Resgates / PL do próprio dia |
| `taxa_fluxo_liquido_pl` | double | Pode ser negativa, zero ou positiva | Fluxo líquido / PL |
| `vl_patrim_liq_d1` | double | PL anterior quando disponível | PL do registro anterior |
| `taxa_resgate_sobre_pl_anterior` | double | >= 0 quando calculada com PL anterior positivo | Resgates / PL anterior |
| `variacao_pl_d1` | double | Com PLs positivos, mínimo teórico -1 e sem limite superior fixo | Variação relativa do PL |
| `p95_amostra_taxa_resgate` | double | Valor >= 0 da distribuição válida | Percentil 95 |
| `evento_extremo_p95` | integer | 0 ou 1 | 1 quando supera o P95 |
| `is_valid_base` | boolean | true/false | Flag herdada da Silver |

#### Gold resumo — consolidação

| Campo | Tipo lógico | Domínio / regra | Descrição |
|---|---|---|---|
| `CNPJ_FUNDO_CLASSE` | string | Identificador público | Identificador da classe/fundo |
| `ID_SUBCLASSE` | string / nulo | String ou nulo | Identificador da subclasse |
| `dias_validos` | long | Contagem inteira >= 0 | Quantidade de dias válidos |
| `data_inicial` | date | Data válida do período | Primeira data válida |
| `data_final` | date | Data válida; >= data inicial quando preenchida | Última data válida |
| `pl_medio` | double | Média do PL da base válida; > 0 quando há registros válidos | PL médio |
| `captacoes_periodo` | double | Soma numérica das captações | Captações |
| `resgates_periodo` | double | Soma numérica dos resgates | Resgates |
| `fluxo_liquido_periodo` | double | Pode ser negativo, zero ou positivo | Soma do fluxo líquido |
| `dias_fluxo_negativo` | long | Inteiro >= 0 e <= `dias_validos` | Dias com fluxo negativo |
| `qtd_eventos_extremos_p95` | long | Contagem inteira >= 0 | Eventos acima do P95 |
| `taxa_resgate_acumulada_rel_pl_medio` | double | Razão >= 0; pode ser > 1 por ser acumulada | Resgates acumulados / PL médio |
| `proporcao_dias_fluxo_negativo` | double | Entre 0 e 1, inclusive | Proporção de dias com fluxo negativo |

O catálogo também está registrado no arquivo `sql/catalogo.sql` e foi documentado por meio do screenshot disponível em `docs/imagens/evidencias do catalogo - modelagem e catalogo de dados.PNG`.

A linhagem utilizada é: dados CSV da CVM → Bronze → Silver → Gold diária → Gold resumo. Os campos calculados da Gold são derivados dos dados preparados na Silver.

## 6. Pipeline de Dados e Arquitetura

O pipeline foi organizado em três notebooks, com responsabilidades separadas: o `01_pipeline_cvm_liquidez.py` concentra a ingestão e as transformações Bronze → Silver → Gold; o `02_qualidade_cvm_liquidez.py` faz as verificações de qualidade; e o `03_analise_cvm_liquidez.py` responde às perguntas P1, P2 e P3. Assim, a parte principal do ETL ficou em um notebook, enquanto qualidade e análise foram mantidas em notebooks próprios.

O pipeline organiza os dados em etapas, desde o arquivo público da CVM até as tabelas utilizadas na análise.

O fluxo utilizado foi:

`dados públicos CVM → Bronze → Silver → Gold diária → Gold resumo → análise`

Cada camada tem uma função diferente no tratamento dos dados.

### Bronze

`bronze_informe_diario`

A Bronze é a primeira tabela do pipeline. Ela recebe os dados dos arquivos CSV da CVM e mantém os campos da origem utilizados no projeto.

Além dos dados recebidos, são registradas informações sobre o arquivo de origem e o momento da ingestão. Essa camada serve como referência para comparar o que foi recebido com o que foi transformado nas etapas seguintes.

### Silver

`silver_informe_diario`

A Silver é a camada de organização e preparação dos dados para os cálculos.

Nela, os campos utilizados no projeto recebem seus tipos corretos. Também são criadas a `ID_SUBCLASSE_CHAVE` e as marcações `has_duplicate_key` e `is_valid_base`.

As duplicidades identificadas são tratadas nessa etapa. A Silver também mantém os registros que não atendem aos critérios de validade, permitindo que eles sejam identificados sem serem confundidos com registros válidos para os indicadores.

### Gold diária

`gold_indicadores_liquidez_diarios`

A Gold diária transforma os dados preparados na Silver em indicadores de liquidez calculados para cada dia.

Nessa etapa são calculados o fluxo líquido, as taxas de resgate e de fluxo em relação ao patrimônio líquido, o patrimônio líquido do dia anterior e a variação do PL.

Também é calculado o P95 da taxa de resgate na amostra e criado o campo `evento_extremo_p95`, usado para identificar os valores acima desse ponto de referência.

Essa tabela é a base das análises que dependem do comportamento diário dos fundos.

### Gold resumo

`gold_resumo_liquidez_fundo`

A Gold resumo reúne os resultados por fundo/classe e período, em vez de manter uma linha para cada dia.

Ela consolida informações como quantidade de dias válidos, período analisado, PL médio, captações, resgates, fluxo líquido, quantidade de dias com fluxo negativo e quantidade de eventos acima do P95.

A partir desses dados consolidados são calculados os indicadores usados nas perguntas P1 e P2. A tabela também facilita a comparação entre os fundos e a apresentação dos resultados na etapa de análise.

### Relação entre as camadas

A organização em Bronze, Silver e Gold permite acompanhar a transformação dos dados ao longo do pipeline:

- Bronze: dados recebidos da fonte;
- Silver: dados organizados e preparados para os cálculos;
- Gold diária: indicadores calculados por dia;
- Gold resumo: resultados consolidados para a análise.

Dessa forma, o projeto mantém uma sequência clara entre origem, tratamento, cálculo e análise dos dados.

### Evidências da persistência na nuvem

As tabelas das camadas foram gravadas no Databricks em formato Delta. As evidências abaixo mostram a persistência das principais etapas do pipeline:

![Tabela Bronze persistida no Databricks](docs/imagens/p1c4%20-%20somente%20tabela%20bronze%20-%20modelagem%20e%20ou%20pipeline%20de%20dados.PNG)

![Tabela Silver persistida no Databricks](docs/imagens/p1c5%20-%20silvergravada%20%2B%20tabela%20silver%20-%20tipagem%20e%20qualidade%20basica%20.PNG)

![Tabela Gold diária persistida no Databricks](docs/imagens/p1c6%20-%20percentil%2095%20e%20tabela%20gold.PNG)

![Tabela Gold resumo persistida no Databricks](docs/imagens/p1c7%20-%20resumo%20da%20gold.PNG)

## 7. Indicadores

Os indicadores foram criados para observar o comportamento de captações, resgates e fluxo de recursos dos fundos durante o período analisado.

### Fluxo líquido

O fluxo líquido mostra se, em determinado dia, entrou ou saiu mais dinheiro do fundo.

```
fluxo_liquido = CAPTC_DIA - RESG_DIA
```

Quando o resultado é positivo, as captações foram maiores que os resgates. Quando é negativo, os resgates foram maiores que as captações, indicando uma saída líquida de recursos naquele dia.

### Taxa de resgate sobre o PL

Essa taxa mostra o tamanho dos resgates de um dia em relação ao patrimônio líquido do fundo naquele mesmo dia.

```
taxa_resgate_pl = RESG_DIA / VL_PATRIM_LIQ
```

Por exemplo, um resultado de 0,05 representa resgates equivalentes a aproximadamente 5% do PL daquele dia. Esse indicador ajuda a comparar o tamanho dos resgates entre fundos de diferentes tamanhos.

### Taxa de fluxo líquido sobre o PL

Esse indicador relaciona o fluxo líquido do dia com o patrimônio líquido do fundo.

```
taxa_fluxo_liquido_pl = (CAPTC_DIA - RESG_DIA) / VL_PATRIM_LIQ
```

O resultado mostra o tamanho da entrada ou saída líquida em relação ao PL. Valores negativos representam saída líquida e valores positivos representam entrada líquida.

### Taxa de resgate sobre o PL do dia anterior

Esse indicador compara o valor resgatado no dia com o patrimônio líquido informado no dia anterior.

```
taxa_resgate_sobre_pl_anterior = RESG_DIA / VL_PATRIM_LIQ_D1
```

A comparação com o PL anterior ajuda a observar o tamanho do resgate em relação à base patrimonial existente antes do movimento daquele dia. Quando não existe PL válido no dia anterior, o indicador não é calculado.

### P95 da taxa de resgate

O P95 foi usado como uma referência estatística para identificar valores muito altos dentro da própria amostra analisada.

Na prática, o P95 representa um valor abaixo do qual ficam aproximadamente 95% das observações válidas. Os casos acima desse valor foram marcados no campo `evento_extremo_p95`.

O P95 encontrado na amostra foi de aproximadamente 0,43%. Isso significa que valores acima desse nível foram considerados extremos em relação à distribuição observada no período analisado.

Esse indicador é uma referência estatística criada para o MVP. Ele não representa um limite regulatório da CVM e não deve ser interpretado, isoladamente, como uma classificação de risco do fundo.

### Índice de resgates acumulados sobre o PL médio

Para a análise P1, foi usado o total de resgates do período dividido pelo PL médio do fundo.

```
taxa_resgate_acumulada_rel_pl_medio = RESGATES_PERIODO / PL_MEDIO
```

Esse indicador mostra o tamanho dos resgates acumulados ao longo do período em relação ao PL médio observado. Como os resgates são somados em vários dias, o resultado pode ser maior que 1 e até muito maior que 1.

Por isso, um resultado de 0,10 significa que os resgates acumulados equivalem a 10% do PL médio, enquanto um resultado de 46,31 significa que o total de resgates acumulados foi 46,31 vezes o PL médio do período. Isso não significa que o fundo tenha perdido 46 vezes o seu patrimônio em um único resgate.

### Proporção de dias com fluxo líquido negativo

Na análise P2, foi calculada a proporção de dias válidos em que o fluxo líquido foi negativo.

```
proporcao_dias_fluxo_negativo = dias_fluxo_negativo / dias_validos
```

Esse indicador mostra com que frequência o fundo apresentou saída líquida de recursos durante o período. Uma proporção de 100% significa que, em todos os dias válidos considerados, os resgates foram maiores que as captações.

Os indicadores foram usados em conjunto para responder às perguntas do MVP e observar diferentes aspectos do comportamento de liquidez. Eles são medidas descritivas e estatísticas da amostra analisada, e não representam uma classificação regulatória de risco.

## 8. Qualidade dos dados

A etapa de qualidade foi usada para verificar se os dados carregados estavam completos, se havia registros duplicados ou inválidos e se existiam valores que poderiam afetar os indicadores.

As verificações foram feitas nas tabelas do pipeline e os resultados foram comparados com as contagens e consultas apresentadas nas evidências do projeto. Quando um problema exigia tratamento para os cálculos, a regra foi aplicada na Silver e pode ser observada no código do notebook principal.

### Completude dos dados

Foi verificado se os principais campos usados nos indicadores estavam preenchidos.

Na Silver, foram analisados os atributos usados no pipeline: CNPJ da classe, identificador da subclasse, data, patrimônio líquido, captações, resgates e número de cotistas. Também foram considerados os metadados de origem e ingestão e, nas camadas derivadas, os campos calculados e flags de controle.

Os resultados foram:

- 1.119.383 registros analisados;
- CNPJ da classe nulo: 0;
- identificador da subclasse nulo: 1.084.238;
- data nula: 0;
- patrimônio líquido nulo: 0;
- captação nula: 0;
- resgate nulo: 0.

A grande quantidade de identificadores de subclasse nulos está presente nos dados analisados. Para permitir a formação da chave usada nas verificações, o pipeline cria o campo `ID_SUBCLASSE_CHAVE`, substituindo o valor nulo por `__SEM_SUBCLASSE__`. O campo original `ID_SUBCLASSE` é mantido.

### Chaves duplicadas

Foi verificada a existência de mais de um registro para a mesma combinação de CNPJ, subclasse e data.

Na Bronze foram encontrados 3 registros excedentes em relação à Silver:

- Bronze: 1.119.386 registros;
- Silver: 1.119.383 registros.

A regra implementada na Silver mantém somente um registro por chave de CNPJ, subclasse e data. Quando há mais de um registro, é selecionado o registro mais recente pelo horário de ingestão. Essa regra está registrada no notebook principal e a diferença de 3 registros entre Bronze e Silver comprova o efeito do tratamento.

### Valores negativos

Também foi feita uma consulta para identificar valores negativos em patrimônio líquido, captação, resgate e número de cotistas.

Os resultados foram:

- patrimônio líquido negativo: 1.383 registros;
- captação negativa: 0;
- resgate negativo: 0;
- número de cotistas negativo: 0.

Os 1.383 registros com patrimônio líquido negativo foram identificados na verificação de qualidade e permanecem na base. Como não foi definida uma regra para excluir ou alterar esses registros, o valor original é preservado.

### Consistência e acurácia

Também foi verificado se os campos utilizados no pipeline correspondiam ao layout efetivamente recebido da CVM. Um exemplo foi a correção da referência `TP_FUNDO` para `TP_FUNDO_CLASSE`, evitando que uma coluna inexistente fosse usada na Silver. As consultas de qualidade também foram usadas para conferir se os valores extremos já estavam presentes na camada Bronze antes das transformações.

A acurácia de negócio dos valores não foi validada contra uma segunda fonte independente, pois o MVP utiliza a própria base pública da CVM como fonte principal. Como verificação possível dentro do escopo, foram conferidos o alinhamento com o layout da fonte, a presença dos valores extremos já na Bronze e as regras de validade usadas nos cálculos. Por isso, os resultados são apresentados como uma análise dos dados disponíveis, com suas limitações.

### Registros inválidos para os indicadores principais

Foram identificados 4.720 registros que não atendiam aos critérios definidos no pipeline para a base válida dos principais indicadores.

A regra considera válidos os registros que possuem CNPJ, data e patrimônio líquido maior que zero. Essa condição é implementada no campo `is_valid_base`.

Os registros que não atendem a essa condição continuam presentes na Silver. A evidência das contagens mostra que a Silver mantém os 1.119.383 registros, enquanto as etapas de cálculo utilizam a condição de validade quando o indicador depende de um patrimônio líquido válido.

### Valores extremos nos indicadores

Foi calculado o P95 da taxa de resgate sobre o patrimônio líquido do dia anterior para identificar valores altos dentro da amostra.

O P95 encontrado foi de aproximadamente 0,43%. Os registros acima desse valor recebem a marcação `evento_extremo_p95 = 1`.

Foram encontrados 54.476 eventos acima do P95 na base analisada.

As consultas de qualidade também permitiram verificar exemplos desses valores na Bronze, mostrando que os dados de origem já apresentavam movimentos de resgate muito altos em relação ao patrimônio líquido informado. Por isso, esses casos são marcados como extremos pelo campo `evento_extremo_p95`.

### Resultado da etapa de qualidade

Depois das transformações, as contagens registradas foram:

| Camada | Registros |
|---|---:|
| Bronze | 1.119.386 |
| Silver | 1.119.383 |
| Gold diária | 1.119.383 |
| Gold resumo | 26.077 |

O período analisado foi de 01/07/2026 a 31/08/2026.

As evidências de qualidade mostram quais problemas foram encontrados e quais tratamentos foram aplicados: na Silver, as duplicidades são identificadas e reduzidas a um registro por chave, é criada uma chave auxiliar para os registros sem subclasse e é criada a marcação `is_valid_base` para indicar se o registro atende aos critérios usados nos cálculos. Os valores negativos de PL e os valores extremos são identificados, mas não são alterados.

## 9. Resultados analíticos

As tabelas do notebook de análise foram usadas para responder diretamente às três perguntas apresentadas no início do MVP.

### P1 — Resgates acumulados sobre PL médio

**Resposta à P1:** os fundos que aparecem no topo da tabela são aqueles que apresentaram a maior razão entre os resgates acumulados e o PL médio no período analisado.

O maior valor observado foi de aproximadamente 46,31 vezes o PL médio, referente ao CNPJ `52.984.696/0001-31`. Na sequência aparecem fundos com índices de aproximadamente 44,23, 44,09, 43,46 e 42,43 vezes o PL médio.

Portanto, a resposta à pergunta P1 é que os maiores índices se concentram em fundos nos quais o volume acumulado de resgates foi muito elevado em relação ao PL médio observado no período. O resultado deve ser interpretado como uma razão acumulada, e não como a afirmação de que o fundo resgatou 46 vezes seu patrimônio em um único evento.

No caso do CNPJ `52.984.696/0001-31`, por exemplo, foram registrados aproximadamente R$ 26,86 milhões em resgates acumulados, enquanto o PL médio observado foi de aproximadamente R$ 579,98 mil. Isso produz a razão de 46,31.

### P2 — Frequência de fluxo líquido negativo

**Resposta à P2:** os fundos que aparecem no topo da tabela apresentaram fluxo líquido negativo em todos os dias válidos considerados no período.

A primeira posição da tabela, assim como os demais fundos destacados no resultado, apresenta 44 dias válidos e 44 dias com fluxo líquido negativo, correspondendo a 100% dos dias considerados. Isso significa que, nesses fundos, em todos os dias válidos analisados, os resgates foram superiores às captações:

`CAPTC_DIA - RESG_DIA < 0`

Como consequência, o fluxo líquido acumulado no período também é negativo. Por exemplo, entre os fundos destacados na tabela, o CNPJ `41.575.707/0001-03` apresentou fluxo líquido acumulado de aproximadamente -R$ 1,03 bilhão.

Assim, a resposta à P2 é que existem fundos com recorrência diária de saída líquida durante todo o período válido analisado. Essa métrica descreve o comportamento observado nos dados e, isoladamente, não caracteriza situação regulatória ou conclusão definitiva sobre risco de liquidez.

### P3 — Eventos acima do P95

**Resposta à P3:** os fundos que aparecem no topo da tabela foram os que concentraram a maior quantidade de eventos de resgate acima do P95 da amostra.

O P95 calculado para a taxa de resgate sobre o PL do dia anterior foi de aproximadamente 0,43%. Os fundos destacados na tabela apresentaram 43 eventos extremos em 43 dias com denominador válido, resultando em 100% dos dias válidos com taxa acima do P95 para esses casos.

Portanto, a resposta à P3 é que os fundos destacados apresentaram uma frequência elevada de eventos classificados como extremos em relação à distribuição observada na própria amostra. Entre os CNPJs que aparecem no resultado estão `05.943.661/0001-74` e `05.114.716/0001-33`.

Esse resultado é apenas uma comparação com os dados da própria amostra. O P95 não é um limite regulatório da CVM e, sozinho, não permite concluir que um fundo esteja em situação de risco de liquidez.

### Discussão geral

As três análises mostram diferentes aspectos do comportamento dos fundos no período estudado: volume acumulado de resgates em relação ao PL médio, frequência de saídas líquidas e ocorrência de resgates extremos em relação à própria amostra. Em conjunto, os indicadores ajudam a identificar fundos e períodos que merecem uma análise mais detalhada. Os resultados, porém, são descritivos e dependem do período e dos dados analisados, não sendo suficientes, isoladamente, para caracterizar risco de liquidez ou situação regulatória.

## 10. Exemplo de série temporal

Foi analisada a série do CNPJ `52.984.696/0001-31` para demonstrar o comportamento dos indicadores ao longo do período.

Em 06/07/2026, por exemplo, foi observado resgate de aproximadamente R$ 26,86 milhões frente a PL anterior de aproximadamente R$ 579,4 mil, resultando em uma taxa de resgate sobre PL anterior de aproximadamente 46,34 vezes, ou cerca de 4.634%.

O exemplo evidencia por que eventos extremos devem ser preservados e investigados, em vez de removidos automaticamente.

## 11. Tratamento de erros

Durante o desenvolvimento, encontrei alguns erros e fui corrigindo-os conforme entendia a causa. As evidências dessas correções foram mantidas no repositório, conforme solicitado na atividade.

### Erro — referência de coluna inexistente

**Problema identificado:** uma etapa do pipeline apresentou erro de resolução de coluna relacionado ao campo `TP_FUNDO`.

**Diagnóstico:** o layout utilizado pela CVM emprega `TP_FUNDO_CLASSE`, e não `TP_FUNDO`.

**Ação corretiva:** a referência foi ajustada para `TP_FUNDO_CLASSE`, mantendo o campo compatível com o layout efetivamente carregado.

**Resultado:** a etapa passou a executar corretamente e a tabela Silver foi gravada.

**Evidências:**

![Erro de referência de coluna](docs/imagens/primeiro%20erro%20-.PNG)

![Correção e resultado](docs/imagens/p1c3%20-%20altera%C3%A7%C3%A3o%20da%20celula%20com%20erro%20e%20resultado.PNG)

### Verificações adicionais de qualidade

Também verifiquei duplicidades, registros inválidos, valores negativos e valores muito altos ou baixos. Essas situações fazem parte da análise da qualidade dos dados.

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

![Duplicidades na Bronze](docs/imagens/p2c2%20_%20chaves%20duplicadas%20bronze.PNG)

![Valores negativos](docs/imagens/p2c3%20-%20consistencia%20_%20valores%20negativos%20que%20deveriam%20ser%20nao%20negativos.PNG)

![Registros inválidos](docs/imagens/p2c4%20-%20registros%20nao%20validos%20para%20os%20indicadores%20principais.PNG)

![Valores extremos](docs/imagens/p2c5%20-%20maiores%20taxas%20de%20resgate%20sobre%20o%20PL%20anterior.PNG)

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

## 14. Como reproduzir o projeto

O projeto foi organizado no Databricks em três notebooks:

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

Como próximos passos, seria interessante ampliar o período analisado e atualizar os dados de forma automática, permitindo acompanhar a evolução dos indicadores ao longo do tempo. Também seria possível incluir outras informações dos fundos e criar uma forma mais simples de acompanhar os casos que apresentassem resultados extremos ou saídas líquidas recorrentes. Essas melhorias permitiriam usar os resultados em análises mais contínuas de monitoramento, controles e risco. O que foi aprendido neste projeto também pode ser aplicado em outras análises de dados.

## 17. Autoavaliação

Este trabalho foi, para mim, uma oportunidade de colocar em prática algo que até então parecia muito distante da minha rotina: transformar dados em uma análise que pudesse responder a perguntas concretas. Durante o desenvolvimento, precisei sair da minha zona de conforto e aprender a lidar com etapas que eu ainda não dominava, desde a organização dos dados até a construção do pipeline e a interpretação dos resultados.

O projeto também me mostrou que fazer uma análise de dados não é apenas executar códigos e olhar para os números. Precisei entender de onde os dados vieram, perceber problemas, investigar resultados que pareciam estranhos e decidir o que deveria ser corrigido, mantido ou apenas sinalizado. Um exemplo foi o erro relacionado à coluna `TP_FUNDO`, que me levou a verificar o layout da fonte e corrigir a referência para `TP_FUNDO_CLASSE`. Outro ponto importante foi aprender a não eliminar automaticamente os valores extremos, mas procurar entender o que eles representavam antes de decidir como tratá-los.

Considero que o principal resultado deste MVP foi ter conseguido acompanhar todo o caminho dos dados, desde a fonte pública até os indicadores utilizados na análise. As três perguntas propostas foram respondidas e, principalmente, consegui compreender melhor como os resultados foram construídos e quais cuidados são necessários para interpretá-los.

Além do aprendizado técnico, vejo uma relação com minha experiência profissional. Trabalho com compliance, controles internos e risco, áreas em que organizar informações e identificar situações fora do esperado é importante. Por isso, acredito que o que aprendi neste MVP pode ser útil tanto para esta disciplina quanto para meu desenvolvimento profissional.

## 18. Referências

A entrega utiliza README e screenshots como documentação e evidência da execução. Não foram utilizados vídeos ou áudios.

- CVM — Fundos de Investimento: Documentos: Informe Diário: https://dados.cvm.gov.br/dataset/fi-doc-inf_diario
- CVM — Fundos de Investimento: Informação Cadastral: https://dados.cvm.gov.br/dataset/fi-cad
- Databricks — Documentação: https://docs.databricks.com/
- Databricks — Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- Databricks — Delta Lake: https://docs.databricks.com/aws/en/delta/
