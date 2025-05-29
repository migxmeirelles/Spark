from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.read.csv("/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/market_data.csv", header=True, sep=",")

nw_df = df.where(f.col("State") == 'New York').select( "MarketName", "city", "County", "State") #--> novo df com apenas New York de Estado, e apenas algumas colunas

#--> contando mercados por cidade, e ordenando desc
nw_df.groupBy("City").count().orderBy("count", ascending=False).show()

#--> contando quantas cidades unicas tem mercado
nw_df.select("City").distinct().count()

#--> case, onde cria uma nova coluna para o que esta em new york
nw_df = nw_df.withColumn("is_in_nyc", f.when(f.col("City") == 'New York', True).otherwise(False))

#--> filtra aquilo que NAO esta em New York
nw_df.filter(f.col("City") != "New York").select("MarketName", "City", "County").show(10, truncate=False)

#--> usando alias 
nw_df.select(
    f.col("MarketName").alias("Mercado"),
    f.col("City").alias("Cidade"),
    f.col("County").alias("Condado")
).show(10, truncate=False)