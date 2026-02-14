# Databricks notebook source
# MAGIC %md
# MAGIC # **#  Projeto Bike Store**
# MAGIC
# MAGIC -  1° Cria catalogo bikestore em external location
# MAGIC -  2° Criar pastas no container e organização em workspace databricks
# MAGIC - 3° Criar Schema logistics  para receber as tabelas e volumes 
# MAGIC - 4° Criar Volume (bikestore_resource) com a Origem dos Dados (deletar todos anteriores para nao ter possivel conflito)
# MAGIC - 5° Criar Pipiline de dados completa com orquestração
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar o SCHEMA 
# MAGIC CREATE SCHEMA IF NOT EXISTS bikestore.logistics;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Criar o SCHEMA E O VOLUME CASO DE ERRO RODE O COMANDO ABAIXO
# MAGIC CREATE SCHEMA IF NOT EXISTS bikestore.logistics;
# MAGIC
# MAGIC -- Deleta o antigo que deu erro e cria o novo
# MAGIC DROP VOLUME IF EXISTS bikestore.logistics.bikestore_resource;
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME bikestore.logistics.bikestore_resource 
# MAGIC LOCATION 'abfss://bikestore@oliststorage.dfs.core.windows.net/resource/origem/'
# MAGIC COMMENT 'Volume conectado ao Azure';

# COMMAND ----------

#Armazena o nome da conta de armazenamento no Azure, a sua Chave de Acesso (Access Key) e o nome do contêiner onde os dados estão.

account = "bikestorestorage"
chave = "5bR5rD10YbbBLxxxxxxxxxxxxxxxxxxxxxxxx=="
container = "bikestore"

# 1. Configura a conexão  
spark._jsc.hadoopConfiguration().set(f"fs.azure.account.key.{account}.dfs.core.windows.net", chave)

# 2. Lista de nomes de arquivos na azure na origem
lista_arquivos = ["brands", "categories", "customers", "products", "orders", "order_items", "staffs", "stocks", "stores"]

try:
    for nome in lista_arquivos:
        #Monta a URL completa do arquivo
        path_csv = f"abfss://{container}@{account}.dfs.core.windows.net/resource/origem/{nome}.csv"
        
        print(f"🔄 Ingerindo: {nome}.csv ➡️ bikestore.logistics.{nome}")
        
        # Leitura direta
        df = (spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(path_csv))
        
        # Gravação no Catálogo
        df.write.mode("overwrite").saveAsTable(f"bikestore.logistics.{nome}")
        
    print("\n✅ VITÓRIA TOTAL! Todas as tabelas foram carregadas no catálogo 'bikestore'.")

except Exception as e:
    # Se der erro aqui, verifique se o nome do arquivo no Azure tem algum prefixo
    print(f"❌ Erro ao processar o arquivo {nome}: {e}")

# COMMAND ----------

#Criacao das Variaveis para caminho

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

# MAGIC %md
# MAGIC ## **Fazendo a ELT na camanda bronze**

# COMMAND ----------

#Subir em Massa os processos acima

# 1. Configurações base
account = "bikestorestorage"
chave = "5bR5rD10YbbBLxxxxxxxxxxxxxxxxxxxxxxxx=="
container = "bikestore"

# 2. LIMPEZA CRÍTICA: Remove a configuração corrompida da memória do Spark
spark._jsc.hadoopConfiguration().unset(f"fs.azure.account.key.{account}.dfs.core.windows.net")

# 3. RECONFIGURAÇÃO LIMPA: Injeta a chave novamente no motor Hadoop
spark._jsc.hadoopConfiguration().set(f"fs.azure.account.key.{account}.dfs.core.windows.net", chave)

# 4. Definição dos caminhos
resource_path = f"abfss://bikestore@{account}.dfs.core.windows.net/resource/origem"
bronze_path = f"abfss://bikestore@{account}.dfs.core.windows.net/bronze"

# 5. Lista de tabelas
tabelas = ["brands", "categories", "customers", "products", "orders", "order_items", "staffs", "stocks", "stores"]

# 6. Loop de Processamento em Massa
for nome in tabelas:
    try:
        print(f"🔄 Ingerindo: {nome}.csv...")
        
        # Leitura (Camada Landing/ Resource) 
        df_temp = (spark.read
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option(f"fs.azure.account.key.{account}.dfs.core.windows.net", chave)
                    .load(f"{resource_path}/{nome}.csv"))
        
        # Escrita em Delta (Camada Bronze) - Sobrescreve o que já existe
        (df_temp.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{bronze_path}/{nome}"))
            
        print(f"✅ Tabela {nome} salva com sucesso em formato Delta!")

    except Exception as e:
        print(f"❌ Erro ao processar {nome}: {e}")

print("\n✨ CARGA DA CAMADA BRONZE FINALIZADA!")
