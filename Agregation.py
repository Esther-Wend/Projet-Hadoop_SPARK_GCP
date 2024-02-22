### Importations des bibliotheques

import os
import matplotlib.pyplot as plt
from pyspark.rdd import RDD
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as func
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window
import sys
from datetime import datetime
from pyspark.sql.functions import to_timestamp, lag, when, unix_timestamp, avg, sum, count


## Creation d'une session SPARK

sc = SparkSession.builder.appName("Test").config("spark.ui.showConsoleProgress", "false").getOrCreate()
sql_c = SQLContext(sc)
sc


# Paramètres passés au script
#DESTINATION = "s3a://formation-christelle-blent-2024/output"  ## (tester )
BUCKET_NAME = "formation-christelle-blent-2024"
Bucket = "gs://{}".format(BUCKET_NAME)


if len(sys.argv) < 4:
    print("Not enough arguments. Usage: script.py <DATE_FROM> <DATE_TO> <DESTINATION>")
    sys.exit(1)

DATE_START  = datetime.fromisoformat(sys.argv[1]).date()
DATE_END  = datetime.fromisoformat(sys.argv[2]).date()
DESTINATION = sys.argv[3]

transaction=Bucket + "/transactions-small.csv"
laundering=Bucket + "/laundering-small.txt"
## ETAPE 1: Développer le script SPark sur un échantillon de données
## Lecture du jeu de données 
data1 = sql_c.read.option("header", True).csv(transaction)
data1.show(5)

data2=sc.read.text(laundering)
data2.show()
### Définissons  le schéma du data2
schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("From Bank", StringType(), True),
    StructField("Account2", StringType(), True),
    StructField("To Bank", StringType(), True),
    StructField("Account4", StringType(), True),
    StructField("Amount Received", DoubleType(), True),
    StructField("Receiving Currency", StringType(), True),
    StructField("Amount Paid", DoubleType(), True),
    StructField("Payment Currency", StringType(), True),
    StructField("Payment Format", StringType(), True),
    StructField("Flag", StringType(), True),
])
 
txt_file_path = laundering
 
# Chargez le fichier texte en tant que DataFrame en utilisant le schéma défini
data2 = sc.read.csv(txt_file_path, schema=schema)
data2.show()

### Exploration du dataframe et ou Prétraitement
## afficher des colonnes
print(data2.columns)
## afficher le schéma des données 
data1.printSchema()
## changement de type de données data1

df1 = data1 \
    .withColumn("Amount Received", func.col("Amount Received").cast("double")) \
    .withColumn("Amount Paid", func.col("Amount Paid").cast("double")) \
    .withColumn("From Bank", func.col("From Bank").cast("int")) \
    .withColumn("To Bank", func.col("To Bank").cast("int"))\
    .withColumn("Timestamp", func.to_timestamp(func.col("Timestamp"), "yyyy/MM/dd HH:mm"))

df1.printSchema()

### recuperer le mois, le jour, l'année
from pyspark.sql.functions import year, month, dayofmonth, hour, minute
df1 = df1.withColumn("Year", year("Timestamp"))\
         .withColumn("Month", month("Timestamp"))\
         .withColumn("Day", dayofmonth("Timestamp"))\
         .withColumn("Hour", hour("Timestamp"))\
         .withColumn("Minute", minute("Timestamp"))

###  Afficher le DataFrame avec les informations temporelles extraites
df1.show()
### donner un résumé statistique des données
df1.describe().show()
### compter le nombre total de lignes dans le DataFrame
print("le nombre total de lignes:",df1.count())

### Suppression des lignes contenant des valeurs manquantes dans df1
df1_cleaned = df1.dropna()

### Affichage du nombre de lignes avant et après la suppression des valeurs manquantes
print(f"Nombre de lignes avant la suppression des valeurs manquantes : {df1.count()}")
print(f"Nombre de lignes après la suppression des valeurs manquantes : {df1_cleaned.count()}")

## Le nombre total  de transaction par compte bancaire
transaction_count=df1.groupBy("Account2").count()
transaction_type_format=df1.groupBy("Payment Format").count()
## Afficher le résultat
transaction_count.show(5)
transaction_type_format.show(5)

### Agrégations et Création de la table de sortie

# Définissez la spécification de la fenêtre pour calculer le délai entre les transactions
window_spec = Window.partitionBy("Account2","Payment Currency", "Receiving Currency").orderBy("Timestamp")

# Ajoutez la colonne PrevTimestamp pour obtenir le timestamp de la transaction précédente
df1_cleaned1 = df1_cleaned.withColumn("PrevTimestamp", F.lag("Timestamp").over(window_spec))

# Calculez le délai entre le timestamp actuel et le timestamp précédent
diff = df1_cleaned1.withColumn("TransactionDelay", when(col("PrevTimestamp").isNotNull(),
                                                         (col("Timestamp").cast("long") - col("PrevTimestamp").cast("long"))))


# Calculer les agrégations nécessaires
aggregations = diff.groupBy("Account2","Payment Currency", "Receiving Currency").agg(
    F.count("Timestamp").alias("num_transactions"),
    F.avg("TransactionDelay").alias("avg_delay_transactions"),
    F.sum("Amount Paid").alias("withdrawals"),
    #F.max(F.when(F.col("Amount Received") == F.col("Amount Paid"), 1).otherwise(0)).alias("has_laundering"),
    F.sum("Amount Received").alias("received"),
)

# Identifiez les comptes suspects à partir des transactions suspectes
suspect_accounts = data2.filter(col("Flag") == "1").select("Account2").distinct()

# Marquez les comptes suspects dans les agrégations
aggregations_with_suspects = aggregations.withColumn(
    "has_laundering",
    col("Account2").isin([row.Account2 for row in suspect_accounts.collect()])
)
aggregations_with_suspects.show(5)


## ETAPE 2

# Filtrer les données en fonction des dates
filtered_data = df1_cleaned.filter((F.col("Timestamp") >= F.lit(DATE_START)) & (F.col("Timestamp") <= F.lit(DATE_END)))
window_spec = Window.partitionBy("Account2","Payment Currency", "Receiving Currency").orderBy("Timestamp")
df1_cleaned1 = filtered_data.withColumn("PrevTimestamp", F.lag("Timestamp").over(window_spec))

# Calculez le délai entre le timestamp actuel et le timestamp précédent
diff = df1_cleaned1.withColumn("TransactionDelay", when(col("PrevTimestamp").isNotNull(),
                                                         (col("Timestamp").cast("long") - col("PrevTimestamp").cast("long"))))
                                                       


# Calculer les agrégations nécessaires
aggregations = diff.groupBy("Account2", "Payment Currency","Receiving Currency").agg(
    F.count("Timestamp").alias("num_transactions"),
    F.avg("TransactionDelay").alias("avg_delay_transactions"),
    F.sum("Amount Paid").alias("withdrawals"),
    F.sum("Amount Received").alias("received"),
)


# Identifiez les comptes suspects à partir des transactions suspectes
suspect_accounts = data2.filter(col("Flag") == "1").select("Account2").distinct()

# Marquez les comptes suspects dans les agrégations
aggregations_with_suspects = aggregations.withColumn(
    "has_laundering",
    col("Account2").isin([row.Account2 for row in suspect_accounts.collect()])
)
# Enregistrez le résultat dans un système cible en fonction de DESTINATION
aggregations_with_suspects.write.mode("overwrite").csv(DESTINATION)


## ETAPE 3
## tester sur Dataproc
sc.stop()
## gs://formation-christelle-blent-2024/sortie.csv