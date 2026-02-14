# Lankehouse-BikeStore

# Data Lakehouse BikeStore: Modernização de Dados SQL Server

##00.Context and Objectives
Este projeto demonstra a migração e transformação de dados de um sistema legado de vendas de bicicletas (SQL Server) para uma arquitetura de **Data Lakehouse** no Azure, utilizando **Databricks** e **Azure Cloud**.

## 01.Arquitetura Medallion

O projeto foi desenvolvido no Azure utilizando os seguintes componentes:
*   **Storage:** Azure Data Lake Storage (ADLS) Gen2.
*   **Compute:** Azure Databricks.
*   **Storage Format:** Delta Lake (Parquet).

Governança e Armazenamento (Unity Catalog)
A organização dos dados segue o modelo de governança do Unity Catalog, estruturado em três níveis: `catalog.schema.table`.

*   **Catalog:** `bikestore_prod` (Isolamento do ambiente de produção).
*   **Schemas:** Divisão lógica por camadas (`bronze`, `silver`, `gold`).
*   **Volumes:** Utilização de **Unity Catalog Volumes** para o armazenamento de arquivos brutos (landing zone) vindos do SQL Server, eliminando a necessidade de mounts legados.


O pipeline foi estruturado em três camadas principais:

0.  **Origem: Ingestão fiel dos dados de origem do SQL Server
1.  **Bronze :** Espelho da Ingestão fiel dos dados de origem do SQL Server em formato Delta.
2.  **Silver (Curated):** Processo de **Data Curation**. Padronização de nomes, limpeza de registros e tipagem de dados.
3.  **Gold (Analytics):** Modelagem Dimensional (Star Schema) pronta para consumo por analistas de BI e áreas de negócio.

## 02. Tecnologias Utilizadas
- **Azure Data Lake Storage (ADLS Gen2)**
- **Azure Databricks** (Spark SQL & PySpark)
- **Delta Lake** (Garantia de transações ACID)
- **SQL Server** (Fonte de dados original)

## 03. Modelagem Gold (DW)
As tabelas finais disponíveis para consumo são:
-  fto_vendas: Transações de vendas consolidadas.
-  fto_order: Transações de pedidos consolidadas.
-  dim_produtos: Catálogo de bicicletas e acessórios.
-  dim_lojas: Informações geográficas das unidades da BikeStore.
-  dim_order_items: Informações de itens cadastrados da BikeStore.
-  dim_customers: Informações cadastrais dos clientes:
