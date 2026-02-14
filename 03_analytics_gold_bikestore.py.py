# Databricks notebook source
# MAGIC     %md
# MAGIC ## # **Processo de ETL GOLD**
# MAGIC
# MAGIC - Criando Curated Data.
# MAGIC - Apos as Transformações salva na camada Gold

# COMMAND ----------

# MAGIC %md
# MAGIC Soma total de vendas por dia , ordens com status de 'Delivered' e estate = NY

# COMMAND ----------

#Pedido para New Your

%python
df_sales_ny_gold = spark.sql("""
                               
select 
  shipped_date
  ,round(sum(total_sale),2) as total_sale
  --,state,status
 from slv_sap_orders
 where state = 'NY'
 and status = 'Delivered'
 and shipped_date is not null
 group by shipped_date
 --,state  ,status
         
                              
                              """)

# salvar em Delta na gold
df_sales_ny_gold.write\
    .mode('overwrite')\
    .format('delta')\
    .option('mergeSchema','true')\
    .save(f'{gold_path}/fto_sales')

# COMMAND ----------

#Ordens Pendentes
%python
df_orders_pending = spark.sql("""
                               
WITH pending AS(   

SELECT  
  customer_id
  ,order_date
  ,sum(quantity) quantity
  ,store_name
FROM tmp_orders
WHERE 1=1
AND lower(status) = 'pending' 
--and lower(status) in ('pending','delivered') 
GROUP BY  customer_id,store_name,order_date
 --,status

)
SELECT 
  p.*
  ,c.first_name as first_name_customer
  ,c.email
  ,c.phone

FROM pending  P
LEFT JOIN  tmp_customers c ON P.customer_id = c.customer_id
WHERE c.email IS NOT NULL
AND c.phone IS NOT NULL
         
                              
                              """)

# salvar em Delta na gold
df_orders_pending.write\
    .mode('overwrite')\
    .format('delta')\
    .option('mergeSchema','true')\
    .save(f'{gold_path}/fto_orders_pending')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disparo de Email
# MAGIC ##

# COMMAND ----------



    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls() 
        server.login(remetente, senha_app)
        server.sendmail(remetente, destinatario, msg.as_string())
        server.quit()
        print(f"Sucesso! E-mail enviado para {destinatario}")
    except Exception as e:
        print(f"Erro ao enviar: {e}")

# Executa a função
enviar_email_sucesso()