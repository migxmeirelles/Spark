from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import count, desc, countDistinct, col

#criando dataFrame atraves de csv
spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.read.csv("/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/market_data.csv",header=True, sep=",")

#selecionando apenas algumas colunas, e as renomeando
df = df.select(
        f.col("MarketName").alias("Mercado"),
        f.col("State").alias("Estado"),
        f.col("City").alias("Cidade")
    )

df = df.na.drop() #--> removendo qualquer null do dataFrame
#df.count() #--> dei um count antes de remover null, e depois, para verificar n° de linhas

#quantos mercados por cidade
df.groupBy("Cidade", "Estado").agg(count("*").alias("Qntd_Mercado")) \
  .orderBy("Estado", desc("Qntd_Mercado")) \
  .show()

#cidades distintas por estado
df.groupBy("Estado") \
  .agg(countDistinct("Cidade").alias("N Cidade")) \
  .orderBy("N Cidade", ascending=False) \
  .show()