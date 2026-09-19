# MVP — Indicadores de Risco de Liquidez em Fundos de Investimento

## 1. Objetivo

Este MVP mostra, na prática, como usei dados públicos da Comissão de Valores Mobiliários (CVM) para fazer uma análise sobre liquidez em fundos de investimento.

O projeto foi desenvolvido no Databricks, usando os recursos disponíveis na plataforma para carregar, organizar e analisar os dados. O código utilizado está no GitHub.

Os indicadores apresentados servem para observar os dados e encontrar situações que mereçam uma análise mais detalhada. Eles não representam uma classificação regulatória de risco nem uma recomendação de investimento.

## 2. Fonte dos dados

A fonte principal é o conjunto Fundos de Investimento: Documentos: Informe Diário, do Portal Dados Abertos da CVM:

https://dados.cvm.gov.br/dataset/fi-doc-inf_diario

O Informe Diário contém, entre outras informações, patrimônio líquido, valor da cota, captações, resgates e número de cotistas.

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

O catálogo de dados está registrado em `sql/catalogo.sql` e também foi documentado por meio do screenshot disponível em `docs/imagens/evidencias do catalogo - modelagem e catalogo de dados.PNG`.

O dicionário da fonte contempla informações como tipo de fundo/classe, identificador, subclasse, data de competência, valor total da carteira, patrimônio líquido, valor da cota, captações, resgates e número de cotistas.

De forma resumida, os dados passam pelos seguintes passos: os arquivos da CVM entram na Bronze; depois são organizados e validados na Silver; na Gold são calculados os indicadores; e, por fim, os resultados são reunidos por fundo. Essas etapas estão descritas no `sql/catalogo.sql`.

## 6. Pipeline de Dados e Arquitetura

O fluxo utilizado foi:

`dados públicos CVM → Bronze → Silver → Gold diária → Gold resumo → análise`

### Bronze

`bronze_informe_diario`

Guarda os dados recebidos e registra informações sobre o arquivo de origem.

### Silver

`silver_informe_diario`

Nesta etapa, organizei os tipos dos campos, tratei a ausência de `ID_SUBCLASSE`, verifiquei duplicidades, retirei as duplicidades identificadas e marquei os registros que poderiam ser usados nos principais cálculos.

### Gold diária

`gold_indicadores_liquidez_diarios`

Nesta etapa, foram calculados os indicadores diários, incluindo fluxo líquido, taxas de resgate, patrimônio líquido do dia anterior e identificação de eventos acima do P95 da amostra.

### Gold resumo

`gold_resumo_liquidez_fundo`

Aqui os resultados são reunidos por fundo/classe para facilitar a resposta às perguntas P1, P2 e P3.

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

A etapa de qualidade foi usada para verificar se os dados carregados estavam completos, se havia registros duplicados ou inválidos e se existiam valores que poderiam distorcer os indicadores.

As verificações foram realizadas antes e depois das transformações do pipeline. Quando foi identificado um problema que afetava os indicadores principais, foi feito um tratamento na camada Silver. Quando o valor poderia representar uma situação real da fonte, ele foi mantido e apenas sinalizado para análise.

### Completude dos dados

A primeira verificação foi feita para identificar campos importantes que estavam vazios.

Na tabela Silver, foram analisados o CNPJ da classe, o identificador da subclasse, a data, o patrimônio líquido, as captações e os resgates.

Os resultados foram:

- 1.119.383 registros analisados;
- CNPJ da classe nulo: 0;
- identificador da subclasse nulo: 1.084.238;
- data nula: 0;
- patrimônio líquido nulo: 0;
- captação nula: 0;
- resgate nulo: 0.

A grande quantidade de identificadores de subclasse nulos não foi tratada como erro. Para permitir a identificação dos registros, foi criada a chave `ID_SUBCLASSE_CHAVE`, que utiliza o valor `__SEM_SUBCLASSE__` quando o campo original está vazio.

Dessa forma, os registros continuam disponíveis no pipeline sem alterar o dado original.

### Chaves duplicadas

Foi verificado se existiam mais de um registro para a mesma combinação de CNPJ, subclasse e data.

Na Bronze foram encontrados 3 registros duplicados. Eles correspondiam a 3 linhas excedentes em relação à Silver:

- Bronze: 1.119.386 registros;
- Silver: 1.119.383 registros.

Na camada Silver, foi mantido apenas um registro para cada combinação de CNPJ, subclasse e data, usando o registro mais recente de acordo com o horário de ingestão.

Esse tratamento evita que o mesmo dia seja contado duas vezes nos indicadores.

### Valores negativos

Também foi verificado se existiam valores negativos em campos que, no contexto deste MVP, não deveriam apresentar esse comportamento.

Foram encontrados:

- patrimônio líquido negativo: 1.383 registros;
- captação negativa: 0;
- resgate negativo: 0;
- número de cotistas negativo: 0.

Os valores negativos de patrimônio líquido foram mantidos porque o objetivo desta etapa foi identificar e registrar o comportamento existente na fonte, e não criar uma regra que pudesse eliminar informações sem uma justificativa adicional.

Já captações, resgates e número de cotistas não apresentaram valores negativos.

### Registros inválidos para os indicadores principais

Foram identificados 4.720 registros que não atendiam aos critérios básicos para o cálculo dos principais indicadores.

O principal caso observado foi o patrimônio líquido igual a zero. Como as taxas de resgate e fluxo são calculadas em relação ao patrimônio líquido, esses registros não podem ser usados de forma segura nessas divisões.

Por isso, a Silver possui o campo `is_valid_base`, que identifica os registros que possuem CNPJ, data e patrimônio líquido válido e maior que zero.

Os registros inválidos não foram apagados da Silver. Eles permanecem disponíveis para rastreabilidade, mas não participam dos cálculos que dependem de uma base patrimonial válida.

### Valores extremos nos indicadores

Depois do tratamento básico, também foram procurados valores muito altos na taxa de resgate.

Para isso, foi calculado o percentil 95 (P95) da taxa de resgate sobre o patrimônio líquido do dia anterior. O valor encontrado na amostra foi de aproximadamente 0,43%.

Os registros acima desse valor receberam a marcação `evento_extremo_p95 = 1`.

Foram encontrados 54.476 eventos acima do P95 na base analisada.

É importante observar que esses valores extremos já estavam presentes nos dados da CVM utilizados como origem. Eles não foram criados pelo pipeline.

Alguns casos apresentaram resgates muito altos em relação ao patrimônio líquido informado no dia anterior. Esses registros foram mantidos porque podem representar situações específicas da movimentação do fundo e, para este MVP, o objetivo é identificá-los e analisá-los, e não eliminá-los automaticamente.

### Resultado da etapa de qualidade

Depois dos tratamentos, o pipeline apresentou:

| Camada | Registros |
|---|---:|
| Bronze | 1.119.386 |
| Silver | 1.119.383 |
| Gold diária | 1.119.383 |
| Gold resumo | 26.077 |

O período analisado foi de 01/07/2026 a 31/08/2026.

A etapa de qualidade permitiu separar três situações diferentes: problemas que precisavam ser tratados para evitar duplicidade ou cálculo inválido, campos que estavam ausentes na própria fonte e valores extremos que deveriam ser identificados, mas não necessariamente excluídos.

Essa separação foi importante para manter a rastreabilidade dos dados e evitar que o tratamento de qualidade alterasse indevidamente as informações recebidas da fonte.

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

## 10. Exemplo de série temporal

Foi analisada a série do CNPJ `52.984.696/0001-31` para demonstrar o comportamento dos indicadores ao longo do período.

Em 06/07/2026, por exemplo, foi observado resgate de aproximadamente R$ 26,86 milhões frente a PL anterior de aproximadamente R$ 579,4 mil, resultando em uma taxa de resgate sobre PL anterior de aproximadamente 46,34 vezes, ou cerca de 4.634%.

O exemplo evidencia por que eventos extremos devem ser preservados e investigados, em vez de removidos automaticamente.

## 11. Tratamento de erros e decisões técnicas

Durante o desenvolvimento, encontrei alguns erros e fui corrigindo-os conforme entendia a causa. As evidências dessas correções foram mantidas no repositório, conforme solicitado na atividade.

### Erro 1 — referência de coluna inexistente

**Problema identificado:** uma etapa do pipeline apresentou erro de resolução de coluna relacionado ao campo `TP_FUNDO`.

**Diagnóstico:** o layout utilizado pela CVM emprega `TP_FUNDO_CLASSE`, e não `TP_FUNDO`.

**Ação corretiva:** a referência foi ajustada para `TP_FUNDO_CLASSE`, mantendo o campo compatível com o layout efetivamente carregado.

**Resultado:** a etapa passou a executar corretamente e a tabela Silver foi gravada.

**Evidências:**

![Erro de referência de coluna](docs/imagens/primeiro%20erro%20-.PNG)

![Correção e resultado](docs/imagens/p1c3%20-%20altera%C3%A7%C3%A3o%20da%20celula%20com%20erro%20e%20resultado.PNG)

### Outras verificações de qualidade

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

Como próximos passos, seria interessante analisar um período maior, atualizar os dados de forma automática e acrescentar outras informações dos fundos. Também seria possível criar um acompanhamento das situações que chamassem mais atenção. Essas melhorias poderiam tornar o projeto mais útil para atividades de monitoramento, controles e análise de risco. O aprendizado deste trabalho também pode ser aproveitado em outros projetos de análise de dados.

## 17. Autoavaliação

Este trabalho foi, para mim, uma oportunidade de colocar em prática algo que até então parecia muito distante da minha rotina: transformar dados em uma análise que pudesse responder a perguntas concretas. Durante o desenvolvimento, precisei sair da minha zona de conforto e aprender a lidar com etapas que eu ainda não dominava, desde a organização dos dados até a construção do pipeline e a interpretação dos resultados.

O projeto também me mostrou que fazer uma análise de dados não é apenas executar códigos e olhar para os números. Precisei entender de onde os dados vieram, perceber problemas, investigar resultados que pareciam estranhos e decidir o que deveria ser corrigido, mantido ou apenas sinalizado. Um exemplo foi o erro relacionado à coluna `TP_FUNDO`, que me levou a verificar o layout da fonte e corrigir a referência para `TP_FUNDO_CLASSE`. Outro ponto importante foi aprender a não eliminar automaticamente os valores extremos, mas procurar entender o que eles representavam antes de decidir como tratá-los.

Considero que o principal resultado deste MVP foi ter conseguido acompanhar todo o caminho dos dados, desde a fonte pública até os indicadores utilizados na análise. As três perguntas propostas foram respondidas e, principalmente, consegui compreender melhor como os resultados foram construídos e quais cuidados são necessários para interpretá-los.

Além do aprendizado técnico, vejo uma relação com minha experiência profissional. Trabalho com compliance, controles internos e risco, áreas em que organizar informações e identificar situações fora do esperado é importante. Por isso, acredito que o que aprendi neste MVP pode ser útil tanto para esta disciplina quanto para meu desenvolvimento profissional.

## 18. Referências

- CVM — Fundos de Investimento: Documentos: Informe Diário: https://dados.cvm.gov.br/dataset/fi-doc-inf_diario
- CVM — Fundos de Investimento: Informação Cadastral: https://dados.cvm.gov.br/dataset/fi-cad
- Databricks — Documentação: https://docs.databricks.com/
- Databricks — Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- Databricks — Delta Lake: https://docs.databricks.com/aws/en/delta/
