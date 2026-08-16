# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.lakeylakey3.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.lakeylakey3.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.lakeylakey3.dfs.core.windows.net", "231e1365-fefa-4159-b106-707d95c2b283")
spark.conf.set("fs.azure.account.oauth2.client.secret.lakeylakey3.dfs.core.windows.net", "Service Credential")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.lakeylakey3.dfs.core.windows.net", "https://login.microsoftonline.com/0c599386-7acd-405d-8338-0a84a7baca4d/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC Read Calendar Data

# COMMAND ----------

df_Cal = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_Cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Las demás

# COMMAND ----------

df_Cus = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_PrCat = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_PrSub = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

# COMMAND ----------

df_Pr = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_Re = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_Sales = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_Terr = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC - ## TRASFORMATIONS
# MAGIC En este momento se pasa la información del folder de bronce al de silver, tmb se permite transformar la data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Para corroborar que se instalaron correctamente las herramientas de la celda 1 se usa lo siguiente:

# COMMAND ----------

# MAGIC %md
# MAGIC Para comprobar que las herramientas (funciones y tipos de datos) se cargaron correctamente en la memoria de tu Notebook, puedes hacer un par de pruebas muy rápidas y sencillas.
# MAGIC
# MAGIC Aquí tienes 3 formas de corroborarlo:
# MAGIC
# MAGIC ### 1. La prueba directa (Imprimir el objeto)
# MAGIC
# MAGIC La forma más fácil de saber si Python reconoce una herramienta es pedirle que te diga qué es. En una celda nueva, escribe el nombre de una de las herramientas que importaste y ejecútala:
# MAGIC
# MAGIC ```python
# MAGIC print(col)
# MAGIC print(StringType)
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC * **Si se cargaron correctamente:** Verás un mensaje técnico que confirma que existen. Por ejemplo, te dirá algo como `<function col at 0x...>` o `<class 'pyspark.sql.types.StringType'>`.
# MAGIC * **Si NO se cargaron:** Te saldrá un error rojo diciendo `NameError: name 'col' is not defined` (lo que significa que la herramienta no existe en la memoria).
# MAGIC
# MAGIC ### 2. Usar el comando `dir()` (Ver el inventario de la memoria)
# MAGIC
# MAGIC Python tiene un comando nativo llamado `dir()` que te muestra una lista con absolutamente todas las variables, funciones y herramientas que están cargadas en la memoria de tu sesión actual.
# MAGIC
# MAGIC En una celda nueva, ejecuta solo esto:
# MAGIC
# MAGIC ```python
# MAGIC dir()
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC Como usaste el asterisco (`*`) en tus imports, al ejecutar `dir()` verás que te devuelve una lista larguísima (cientos de palabras) con cosas como `'ArrayType'`, `'DateType'`, `'col'`, `'concat'`, `'lit'`, `'sum'`, etc. Si ves esos nombres ahí, las herramientas están listas para usarse.
# MAGIC
# MAGIC ### 3. La prueba del autocompletado
# MAGIC
# MAGIC Ahora que ejecutaste las celdas de los `import`, intenta escribir una función en una celda nueva, por ejemplo:
# MAGIC
# MAGIC Escribe `StringT` y presiona la tecla **Tab** (o Ctrl + Espacio).
# MAGIC
# MAGIC * Si se cargó correctamente, Databricks completará la palabra a `StringType` o te abrirá un pequeño menú sugiriéndola.
# MAGIC
# MAGIC ### 💡 Un tip básico de Databricks:
# MAGIC
# MAGIC En Databricks, si ejecutas una celda que contiene comandos `import` y la celda termina de ejecutarse mostrando **un visto bueno verde y el tiempo que tardó (ej. `Command took 0.12 seconds`)** sin arrojar ningún texto rojo de error, puedes tener la total seguridad de que la importación fue exitosa.

# COMMAND ----------

!pip list

# COMMAND ----------

print(col)
print(StringType)

# COMMAND ----------

# MAGIC %md
# MAGIC Se confirmó que fueron instaladas correctamente

# COMMAND ----------

# MAGIC %md
# MAGIC Se hacen las transformacion es en la tabla de Calendar y se muestra la tabla cambiada

# COMMAND ----------

df_Cal = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronce@lakeylakey3.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_Cal = df_Cal.withColumn("Month",month(col('Date')))\
    .withColumn("Year",year(col('Date')))
df_Cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Se carga a contenedor "Silver" en el datalake (lakeylakey3)

# COMMAND ----------

df_Cal.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@lakeylakey3.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customers

# COMMAND ----------

# MAGIC %md
# MAGIC Al ser más complicada, la tabla de Customers se ve primero para decidir que transformaciones realizar

# COMMAND ----------

df_Cus.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Se realizan cambios para juntar los nombres y apellidos en una sola columna.
# MAGIC Se creo una columna con el nombre completo. Concatenando la columna "Prefijo" _ Espacio _ columna "FirstName" _ Espacio _ columna "LastName"

# COMMAND ----------

df_Cus = df_Cus.withColumn("FullName",concat(col("Prefix"),lit(" "),col("FirstName"),lit(" "),col("LastName"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Es otra forma de hacer lo mismo, pero en vez de colocar manualmente los espacios, se coloca un comando "concatenar con" y se indica _Espacios_. Entonces sale una función concatenar con espacios y luego se indican las columnas. 
# MAGIC ### PREACAUCIÓN:
# MAGIC Cuando se realice una transformación con "=" no se coloca el ".display()" porque se vuelve parte de la variable y cuando se trata de utilizar más adelante ocaciona error. Hay que ponerla en un linea aparte en un comando aparte

# COMMAND ----------

df_Cus = df_Cus.withColumn("FullName", concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName")))
display(df_Cus )

# COMMAND ----------

df_Cus.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@lakeylakey3.dfs.core.windows.net/AdventureWorks_Customer")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Se revisa complejidad de tabla Product Categoríe y al ver que es mínima se carga directamente

# COMMAND ----------

display(df_PrCat)

# COMMAND ----------

df_PrCat.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@lakeylakey3.dfs.core.windows.net/AdventureWorks_Product_Categories")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Se revisa la complejidad de la tabla de sub categorías y al ver que es mínima se carga directamente al folder "Silver"

# COMMAND ----------

display(df_PrSub)

# COMMAND ----------

df_PrSub.write.format("parquet")\
      .mode("append")\
      .option("path","abfss://silver@lakeylakey3.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
      .save()


# COMMAND ----------

# MAGIC %md
# MAGIC Se revisa products

# COMMAND ----------

display(df_Pr)

# COMMAND ----------

# MAGIC %md
# MAGIC Se decide realizar las siguientes transformaciones:\
# MAGIC 1 - Obtener la información antes del "-" del SKU\
# MAGIC 2 - La primera palabra de la columna "ProductName"\
# MAGIC SE UTILIZA LA FUNCIÓN SPLIT

# COMMAND ----------

df_Pr = df_Pr.withColumn("ProductSKU",split("ProductSKU","-")[0])\
    .withColumn("ProductName",split("ProductName"," ")[0])

# COMMAND ----------

display(df_Pr)

# COMMAND ----------

df_Pr.write.format("parquet")\
      .mode("append")\
      .option("path","abfss://silver@lakeylakey3.dfs.core.windows.net/AdventureWorks_Products")\
      .save()

# COMMAND ----------

display(df_Re)

# COMMAND ----------

df_Re.write.format("parquet")\
      .mode("append")\
      .option("path","abfss://silver@lakeylakey3.dfs.core.windows.net/AdventureWorks_Returns")\
      .save()

# COMMAND ----------

display(df_Terr)

# COMMAND ----------

df_Terr.write.format("parquet")\
      .mode("append")\
      .option("path","abfss://silver@lakeylakey3.dfs.core.windows.net/AdventureWorks_Territories")\
      .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##SALES

# COMMAND ----------

display(df_Sales)

# COMMAND ----------

df_Sales = df_Sales.withColumn("StockDate",to_timestamp("StockDate"))

# COMMAND ----------

df_Sales = df_Sales.withColumn("OrderNumber",regexp_replace(col("OrderNumber"),"S","T"))


# COMMAND ----------

df_Sales.display()

# COMMAND ----------

df_Sales.groupBy("OrderDate").agg(count("OrderNumber").alias("total_order")).display()

# COMMAND ----------

df_Sales.write.format("parquet")\
      .mode("append")\
      .option("path","abfss://silver@lakeylakey3.dfs.core.windows.net/AdventureWorks_Sales")\
      .save()