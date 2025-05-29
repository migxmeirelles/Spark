from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

#--> criando dataframes 
clientes = spark.createDataFrame(
    [
        (1, "Miguel"),
        (2, "Paula")
    ], ["id", "Nome"]
)

compras = spark.createDataFrame(
    [
        (1, 1, "Batata Palha", 7),
        (2, 1, "Refrigerante", 12),
        (3, 2, "Carne", 25),
        (4, 2, "Bombom", 2),
        (5, 1, "Creme de Leite", 4)
    ], ["id","id_cliente", "compra", "valor"]
)

#--> criando join e vendo
df_join = clientes.join(compras, clientes.id == compras.id_cliente, "inner")
df_join.select("Nome", "compra", "valor").show()

#left -> tudo da esquerda
#right -> tudo da direita
#inner -> tudo em comum
#outer -> tudo, mesmo sem match
