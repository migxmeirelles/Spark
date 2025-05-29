from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, count, desc

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

#criando Estados com mais Mercado:
df.groupBy("Estado", "Cidade").agg(count("*").alias("N Mercados")) \
  .orderBy("Estado", desc('N Mercados')) \
  .show()

#trabalhando com Janela e Rank:
janela = Window.partitionBy("Estado").orderBy(desc("N Mercados"))

ranking = df.groupBy("Estado", "Cidade") \
    .agg(count("*").alias("N Mercados")) \
    .withColumn("rank", row_number().over(janela))

ranking.show(3)