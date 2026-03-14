[dbt__BADGE]: https://img.shields.io/badge/<dbt>-<3.11>-orange.svg
[POWERBI__BADGE]: https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black
[GCP__BADGE]: https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white


<h1 align="center" style="font-weight: bold;">Customer Acquisition & Cohort Retention 📊</h1>

<div align="center">
<br />

![gcp][GCP__BADGE]
![dbt][dbt__BADGE]
![powerbi][POWERBI__BADGE]

</div>

## Descrição

Este projeto demonstra a construção de uma plataforma moderna de analytics para e-commerce / SaaS B2C, permitindo analisar aquisição de clientes, geração de receita e retenção ao longo do tempo.

A solução foi desenvolvida utilizando **BigQuery + dbt + Power BI**, seguindo princípios de Analytics Engineering, **arquitetura Medallion** e modelagem dimensional **(Star Schema)**.

O objetivo é criar uma fonte única de verdade para métricas de marketing e receita, permitindo que áreas de Marketing, Growth e Produto analisem:
- Performance de canais de aquisição
- Conversão do funil de marketing
- Receita gerada por campanhas
- LTV dos clientes
- Retenção e comportamento por cohort

## Contexto de Negócio

Empresas digitais dependem de aquisição contínua de usuários e precisam entender:
- Quais canais trazem clientes de maior valor
- Como os usuários evoluem no funil de conversão
- Quanto valor os clientes geram ao longo do tempo
- Qual é o nível de retenção após a primeira compra

Este projeto simula esse cenário criando uma infraestrutura completa de dados e analytics para responder essas perguntas.

A análise é baseada em:
- Jornada do usuário: view → click → add_to_cart → purchase
- Atribuição: Last Click com janela de 30 dias
- Valor do cliente: LTV em 90 dias
- Retenção: baseada em cohort de primeira compra

## Como começar

1️⃣ Clonar repositório
```bash
git clone https://github.com/VictorDantas19/marketing_analytics_dbt
```

2️⃣ Instalar dbt
```bash
pip install dbt-bigquery
```
3️⃣ Configurar profile com credenciais do BigQuery.

Editar:
```bash
~/.dbt/profiles.yml
```
4️⃣ Rodar modelos
```bash
dbt run
```
5️⃣ Executar testes


## Arquitetura do Projeto
O pipeline segue uma adaptação da arquitetura Medallion, organizada em quatro camadas.

```bash
Raw Data (CSV)
      │
      ▼
    Bronze
(replica da fonte)
      │
      ▼
    Staging
(tratamentos mínimos + tipos)
      │
      ▼
    Intermediate
(regras de negócio + enriquecimento)
      │
      ▼
    Gold
(star schema para analytics)
      │
      ▼
Power BI Dashboard
```

### Camadas
Bronze
- Replica fiel da fonte
- Auditoria e rastreabilidade

Staging
- Tipagem de colunas
- Padronizações iniciais
- Ajustes de qualidade de dados

Intermediate
- Implementação de regras de negócio
- Enriquecimento das entidades
- Preparação para métricas

Gold
- Modelo dimensional (Star Schema)
- Tabelas fato e dimensão
- Prontas para consumo em BI

### Stack Tecnológica

| Camada         | Ferramenta                     |
| -------------- | ------------------------------ |
| Data Warehouse | **Google BigQuery**            |
| Transformação  | **dbt**                        |
| Modelagem      | **SQL + Dimensional Modeling** |
| Orquestração   | **dbt**                        |
| Visualização   | **Power BI**                   |
| Versionamento  | **Git + GitHub**               |


### Estrutura do Repositório

```bash
marketing_analytics_dbt
│
├── models
│   ├── staging
│   │   ├── stg_events.sql
│   │   ├── stg_transactions.sql
│   │   └── stg_customers.sql
│   │
│   ├── intermediate
│   │   ├── int_valid_transactions.sql
│   │   ├── int_attribution_last_click_30d.sql
│   │   ├── int_customer_enriched.sql
│   │   └── int_transactions_enriched.sql
│   │
│   └── marts
│       ├── dim_customers.sql
│       ├── dim_products.sql
│       ├── dim_campaigns.sql
│       ├── dim_dates.sql
│       ├── fact_transactions.sql
│       ├── fact_events.sql
│       ├── fact_funnel_daily.sql
│       └── fact_cohort_monthly.sql
```

### Fontes de Dados

O projeto utiliza 5 datasets simulados em CSV.

| Tabela       | Linhas    | Granularidade         |
| ------------ | --------- | --------------------- |
| products     | 2.000     | 1 linha por produto   |
| campaigns    | 50        | 1 linha por campanha  |
| customers    | 100.000   | 1 linha por cliente   |
| transactions | 103.127   | 1 linha por transação |
| events       | 2.000.000 | 1 linha por evento    |

### Modelo Dimensional (Star Schema)
O modelo final segue o padrão Star Schema, separando fatos e dimensões.

Dimensões
- dim_customers
- dim_products
- dim_campaigns
- dim_dates

Tabelas Fato
- fact_transactions
- fact_events
- fact_funnel_daily
- fact_cohort_monthly

```bash
                dim_customers
                       │
                       │
dim_products ── fact_transactions ── dim_campaigns
                       │
                       │
                   dim_dates


                dim_customers
                       │
                       │
dim_products ── fact_events ── dim_campaigns
                       │
                       │
                   dim_dates

dim_dates ── fact_funnel_daily ── dim_campaigns

dim_dates ── fact_cohort_month
```

## Testes de Qualidade (dbt)

Foram implementados testes para garantir qualidade dos dados.

Testes principais
- not_null
- unique
- relationships
- accepted_values

Exemplo:
```bash
tests:
  - unique:
      column_name: transaction_id

  - not_null:
      column_name: customer_id
```

## Documentação Completa

[📝 Documentação no LinkedIn](https://www.atlassian.com/br/git/tutorials/making-a-pull-request)

[LinkedIn do Autor](https://www.linkedin.com/in/victordantas07/)
