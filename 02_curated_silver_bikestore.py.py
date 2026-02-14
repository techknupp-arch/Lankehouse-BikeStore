# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## **Processo de ETL SILVER**
# MAGIC
# MAGIC - Limpeza dos dados.
# MAGIC - Tratativas de Campos.
# MAGIC - Apos as Transformações salva na camada Silver

# COMMAND ----------

#Armazena o nome da conta de armazenamento no Azure, a sua Chave de Acesso (Access Key) e o nome do contêiner onde os dados estão.

account = "bikestorestorage"
chave = "5bR5rD10YbbBLxxxxxxxxxxxxxxxxxxxxxxxx=="
container = "bikestore"

# 1. Configura a conexão  
spark._jsc.hadoopConfiguration().set(f"fs.azure.account.key.{account}.dfs.core.windows.net", chave)

# COMMAND ----------

bronze_path = "abfss://bikestore@oliststorage.dfs.core.windows.net/bronze/"
silver_path = "abfss://bikestore@oliststorage.dfs.core.windows.net/silver/"
gold_path = "abfss://bikestore@oliststorage.dfs.core.windows.net/gold/"
resource_path = "abfss://bikestore@oliststorage.dfs.core.windows.net/resource/"
resource_path_volume = "bikestore.logistics"
#Validacao
print(f"✅ Variavél armazenada com Sucesso!  {bronze_path}")
print(silver_path)
print(gold_path)
print(resource_path)
print(resource_path_volume)

# COMMAND ----------

# Criando em Massa as tabelas temporárias

# 1. Defina a lista de pastas/tabelas que deseja carregar
tabelas = ["brands", "categories", "customers", "products", "orders", "order_items", "staffs", "stocks", "stores"] 

# 2. Loop para ler os dados Delta e criar as Views Temporárias
for tabela in tabelas:
    try:
        path = f"{bronze_path}/{tabela}/"
        (spark.read.format('delta')
          .load(path)
          .createOrReplaceTempView(f'tmp_{tabela}'))
        print(f"Sucesso: View 'tmp_{tabela}' pronta para uso.")
    except Exception as e:
        print(f"Erro ao carregar a tabela {tabela}: {e}")


# COMMAND ----------

    #CRIACAO DA PROD

    from pyspark.sql import functions as F

    # 1. ORIGEM

    #FROM
    df_products = spark.table("tmp_products").alias("p")
    df_categories = spark.table("tmp_categories").alias("c")

    # 2. JOIN e SELECT

    dim_prod = (
                df_products
                .join(
                    df_categories, 
                    on="category_id", 
                    how="left"
                    )
        .select(



# COMMAND ----------

# 1. Configurações de Caminho
silver_path = "abfss://bikestore@oliststorage.dfs.core.windows.net/silver/"
tbl_prod = f"{silver_path}dim_products"

# 2. Salvando o DataFrame slv_prod
(
dim_prod    
    .write
    .format("delta") 
    .mode("overwrite") 
    .option("overwriteSchema", "true") 
    .save(slv_sql_prod)
)

print(f"✅ Dados salvos com sucesso na Silver: {slv_sql_prod}")

# COMMAND ----------

# Ver o histórico da tabela. Validacao
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, tbl_prod)
display(deltaTable.history())


# COMMAND ----------

# Salvando o conteúdo do DataFrame dim_prod como uma tabela física schema logistics no catalago
(
    dim_prod.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bikestore.logistics.slv_sql_prod")
)

print("✅ Tabela 'slv_sql_prod' criada fisicamente em bikestore.logistics")


# COMMAND ----------

# Criando em Massa as tabelas temporárias

# 1. Defina a lista de pastas/tabelas que deseja carregar
tabelas = ["brands", "categories", "customers", "products", "orders", "order_items", "staffs", "stocks", "stores"] 

# 2. Loop para ler os dados Delta e criar as Views Temporárias
for tabela in tabelas:
    try:
        path = f"{bronze_path}/{tabela}/"
        (spark.read.format('delta')
          .load(path)
          .createOrReplaceTempView(f'tmp_{tabela}'))
        print(f"Sucesso: View 'tmp_{tabela}' pronta para uso.")
    except Exception as e:
        print(f"Erro ao carregar a tabela {tabela}: {e}")

# COMMAND ----------

#sum, count, avg, col, when, concat
import pyspark.sql.functions as F
#definir esquemas e tipos de dados (Integer, String, Float)
import pyspark.sql.types as T
# RANK, ROW_NUMBER)
from pyspark.sql.window import Window


# COMMAND ----------

df_product_silver = spark.sql("""
with
stock as ( 
select 
  product_id
  ,sum(quantity) as total_stock
from tmp_stocks
--where product_id = 1
group by product_id
)
select
   p.product_id
  ,p.product_name
  --,p.brand_id
  --,b.brand_id as brand_id_brand
  ,b.brand_name
  --,p.category_id
  --,c.category_id as category_id_categoriat
  ,c.category_name
  ,p.model_year
  ,p.list_price
  ,s.total_stock
from        tmp_products    as p
left join   tmp_categories  as c on p.category_id = c.category_id
left join   tmp_brand       as b on p.brand_id = b.brand_id
left join   stock           as s on p.product_id = s.product_id                             
                              
                              
                              """)
# salvar em Delta na silver 
df_product_silver.write\
    .mode('overwrite')\
    .format('delta')\
    .option('mergeSchema','true')\
    .save(f'{silver_path}/slv_sap_product')