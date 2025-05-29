from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("My First Notbook").getOrCreate()

df = spark.read.csv("/databricks-datasets/COVID/coronavirusdataset/Region.csv",header=True, inferSchema=True)

df.printSchema() #--> visualizar schema, tipo de dados, etc...
df.columns #--> visualizar colunas do dataFrame

df.filter(df["elementary_school_count"] > 500).show() #--> filtrando dados com coluna operando x

df.select("province", "elementary_school_count").show() #--> selecionando apenas algumas colunas

df.orderBy(df["elementary_school_count"].desc()).show() #--> ordenando decresente por uma coluna x

