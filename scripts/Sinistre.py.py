# Databricks notebook source
sc.version

# COMMAND ----------

import datetime 
from decimal import Decimal
import numpy as np
from pyspark.sql.types import *
import pyspark.sql.functions as func
# tu peux rajouté tous les autres import donc tu aurais besoin 

# COMMAND ----------

# Copier le fichier ZIP dans le système de fichiers local
dbutils.fs.cp("dbfs:/FileStore/shared_uploads/mouhamadoub977@gmail.com/TP.zip", "file:/tmp/TP.zip")

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /tmp/TP.zip -d /tmp/TP_unzipped/
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC find /tmp/TP_unzipped/
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /tmp/TP_unzipped/TP/data_input.zip -d /tmp/TP_unzipped/TP/data_input_unzipped/

# COMMAND ----------

df_ocr = spark.read.parquet("file:/tmp/TP_unzipped/TP/data_input_unzipped/ocr_input_test.parquet")
display(df_ocr)

# COMMAND ----------



# COMMAND ----------

# zoning_input_test = spark.read.parquet("file:/tmp/TP_unzipped/TP/data_input_unzipped/zoning_input_test.parquet")
# display(zoning_input_test)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1-1
# MAGIC ## A partir de rdd_exo selectionner seulement les 3 premieres colonnes en utilisant un map
# MAGIC ## Q1-2
# MAGIC ## l'ojectif c'est d'obtenir le meme resultat que la question Q1-1 à partir du rdd_3 en utilisant seulement les fonctions RDD

# COMMAND ----------

import datetime 
from decimal import Decimal

raw_data = [
      ["0000002820037104", 'name1', 0.13, Decimal(0.13), True, datetime.datetime(2018, 1, 1), datetime.datetime(2018, 1, 1)],
      ["0000007552354404", 'name2', None, Decimal(0.23), True, datetime.datetime(2017, 12, 1), datetime.datetime(2018, 1, 1) ],
      ["0000006798765204", 'name3', 0.13, Decimal(0.33), True, datetime.datetime(2017, 11, 1), datetime.datetime(2018, 1, 1) ],
      ["0000007523305204", 'name1', 0.13, Decimal(0.43), True, datetime.datetime(2017, 10, 1), datetime.datetime(2018, 1, 1) ],
      ["0000006956358404", 'name2', 0.13, Decimal(0.53), True, datetime.datetime(1900, 9, 1), datetime.datetime(2018, 1, 1)  ],
      ["0000006232433904", 'name3', None, Decimal(0.63), True, datetime.datetime(2017, 8, 1), datetime.datetime(2018, 1, 1)  ],
      ["0000005818210404", 'name1', 0.13, Decimal(0.73), True, None, None],
      ["0000007952803204", 'name2', 0.13, Decimal(0.83), True, datetime.datetime(2017, 6, 1), None],
      ["0010000323720787", 'name3', 0.13, Decimal(0.93), True, datetime.datetime(2017, 5, 1), None],
      ["0000004244194904", 'name1', None, Decimal(0.103),True, datetime.datetime(2017, 4, 1), None],
      ["0000004244194904", 'name1', 0.13, Decimal(0.103),True, datetime.datetime(2017, 4, 1), None],
      ["0000002820037104", 'name1', None, Decimal(1.13), True, datetime.datetime(2018, 1, 1), None],
      ["0000007552354404", 'name2', 0.13, Decimal(1.23), True, datetime.datetime(2017, 12, 1), None],
      ["0000006798765204", 'name3', 0.13, Decimal(1.33), True, None, None],
      ["0000007523305204", 'name1', None, Decimal(1.43), True, datetime.datetime(2017, 10, 1), None],
      ["0000006956358404", 'name2', 0.13, Decimal(1.53), True, datetime.datetime(2017, 9, 1), None],
      ["0000006232433904", 'name3', 0.13, Decimal(1.63), True, datetime.datetime(2017, 8, 1), None],
      ["0000005818210404", 'name1', 0.13, Decimal(1.73), True, datetime.datetime(2017, 7, 1), None],
      ["0000007952803204", 'name2', 0.13, Decimal(1.83), True, datetime.datetime(2017, 6, 1), None],
      ["0010000323720787", 'name3', None, Decimal(1.93), True, datetime.datetime(2017, 5, 1), None],
      ["0000004244194904", 'name1', 0.13, Decimal(1.103),True, datetime.datetime(2017, 4, 1), None],
      ['0000006232433900', 'name2', 0.13, Decimal(1.113),True, datetime.datetime(2017, 3, 1), None] ]
rdd_exo = sc.parallelize(raw_data)
# display(rdd_exo.collect())

raw_data_3 = [('name3', [ ['0000006798765204', 'name3', 0.13, Decimal('0.33'), True,  datetime.datetime(2017, 11, 1, 0, 0), datetime.datetime(2018, 1, 1, 0, 0)],
        ['0000006232433904', 'name3', None, Decimal('0.63'), True, datetime.datetime(2017, 8, 1, 0, 0), datetime.datetime(2018, 1, 1, 0, 0)],
        ['0010000323720787', 'name3', 0.13, Decimal('0.93'), True, datetime.datetime(2017, 5, 1, 0, 0)],
        ['0000006798765204', 'name3', 0.13, Decimal('1.33'), True, None],
        ['0000006232433904', 'name3', 0.13, Decimal('1.63'), True, datetime.datetime(2017, 8, 1, 0, 0)],
        ['0010000323720787', 'name3', None, Decimal('1.93'), True,  datetime.datetime(2017, 5, 1, 0, 0)]]),
                 ('name1', [ ['0000002820037104', 'name1', 0.13, Decimal('0.13'), True, datetime.datetime(2018, 1, 1, 0, 0), datetime.datetime(2018, 1, 1, 0, 0)],
        ['0000007523305204', 'name1', 0.13, Decimal('0.43'), True, datetime.datetime(2017, 10, 1, 0, 0), datetime.datetime(2018, 1, 1, 0, 0)],
        ['0000005818210404', 'name1', 0.13, Decimal('0.73'), True, None],
        ['0000004244194904', 'name1', None, Decimal('0.103'), True, datetime.datetime(2017, 4, 1, 0, 0)],
        ['0000004244194904', 'name1', 0.13, Decimal('0.103'), True, datetime.datetime(2017, 4, 1, 0, 0)],
        ['0000002820037104', 'name1', None, Decimal('1.13'), True, datetime.datetime(2018, 1, 1, 0, 0)],
        ['0000007523305204', 'name1', None, Decimal('1.43'), True, datetime.datetime(2017, 10, 1, 0, 0)],
        ['0000005818210404', 'name1', 0.13, Decimal('1.73'), True, datetime.datetime(2017, 7, 1, 0, 0)],
        ['0000004244194904', 'name1', 0.13, Decimal('1.103'), True,  datetime.datetime(2017, 4, 1, 0, 0)]]),
               ('name2', [  ['0000007552354404', 'name2', None, Decimal('0.23'), True, datetime.datetime(2017, 12, 1, 0, 0), datetime.datetime(2018, 1, 1, 0, 0)],
        ['0000006956358404', 'name2', 0.13, Decimal('0.53'), True, datetime.datetime(1900, 9, 1, 0, 0), datetime.datetime(2018, 1, 1, 0, 0)],
        ['0000007952803204', 'name2', 0.13, Decimal('0.83'), True, datetime.datetime(2017, 6, 1, 0, 0)],
        ['0000007552354404', 'name2', 0.13, Decimal('1.23'), True, datetime.datetime(2017, 12, 1, 0, 0)],
        ['0000006956358404', 'name2', 0.13, Decimal('1.53'), True, datetime.datetime(2017, 9, 1, 0, 0)],
        ['0000007952803204', 'name2', 0.13, Decimal('1.83'), True, datetime.datetime(2017, 6, 1, 0, 0)],
        ['0000006232433900', 'name2', 0.13, Decimal('1.113'), True, datetime.datetime(2017, 3, 1, 0, 0)]])]
rdd_3 = sc.parallelize(raw_data_3)
# display(rdd_3.collect())


# COMMAND ----------

#A partir de rdd_exo selectionner seulement les 3 premieres colonnes en utilisant un map
rdd_firts3 = rdd_exo.map(lambda row: (row[0], row[1], row[2]))

# display(rdd_firts3.collect())
# display(rdd_firts3.take(10))


# COMMAND ----------

# Obtenir le même résultat avec rdd_3 et les fonctions RDD
#attention listes imbriquées
# print(rdd_3.take(3))

# Utiliser flatMap pour accéder à chaque liste d'enregistrements et extraire les trois premières colonnes
rdd_3_first3 = rdd_3.flatMap(lambda x: [(record[0], record[1], record[2]) for record in x[1]])

# Afficher le résultat
# display(rdd_3_first3.collect())



# COMMAND ----------

# MAGIC %md
# MAGIC ##Q2
# MAGIC ## A partir du nom du fichier "split_filename" on voudrait extraire le document ID ainsi que le page number ex: 
# MAGIC POC_OCR_{6D43D204-4B8B-46EF-940F-8E80C9B32964}.pdf-1.png -> doc_id = 6D43D204-4B8B-46EF-940F-8E80C9B32964 page_number = 1

# COMMAND ----------

import re
split_filename =  "POC_OCR_{6D43D204-4B8B-46EF-940F-8E80C9B32964}.pdf-1.png"

# Utilisation une expression régulière pour extraire le doc_id et le page_number
match = re.search(r"\{([A-Z0-9-]+)\}.pdf-(\d+).png", split_filename)

# Vérifier si le pattern correspond
if match:
    doc_id = match.group(1)
    page_number = int(match.group(2))
    print(f"doc_id = {doc_id}")
    print(f"page_number = {page_number}")
else:
    print("Le format du fichier n'est pas valide.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 
# MAGIC ## Pour chaque document "doc_id" qui n'a PAS qu'un seul recto (colonne "label_zoning_1" valeur "CNI_FACE_photo") et qu'un seul verso (colonne "label_zoning_1" valeur "CNI_FACE_adresse")
# MAGIC ##Determiner le score de zoning "score_zoning_1", le label "label_zoning_1" ansi que la position "ymax" de "coordinates_zoning_1"

# COMMAND ----------

zoning_input_test = spark.read.parquet("file:/tmp/TP_unzipped/TP/data_input_unzipped/zoning_input_test.parquet")
display(zoning_input_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4
# MAGIC ## Afficher la table des documents "doc_id" qui n'ont pas l'ensemble de ces champs "label_zoning_champ" de present ["ID_CNI", "Nom", "Prenom", "mrz_ligne1", "mrz_ligne2"]

# COMMAND ----------

df_zoning = spark.read.parquet('file:/tmp/TP_unzipped/TP/data_input_unzipped/ocr_input_test.parquet')
df_zoning.display()

# COMMAND ----------

from pyspark.sql import functions as F
#calcul du nbre de recto et verso pou chaq doc id
count_labels = (df_zoning.groupBy("doc_id")
                .pivot("label_zoning_1", ["CNI_FACE_photo", "CNI_FACE_adresse"])
                .count()
                .fillna(0))
#filtre pour garder les id qui n'ont pas exactement un "CNI_FACE_photo" et "CNI_FACE_adresse"
doc_ids_to_exclude = count_labels.filter(
  (F.col("CNI_FACE_photo") != 1)
).select("doc_id")
#exraire les info demandées pour les doc_id filtrées
result_df = (df_zoning.join(doc_ids_to_exclude, on="doc_id", how="inner")
.select("doc_id", "score_zoning_1",  "label_zoning_1", F.col("coordinates_zoning_1.ymax").alias("ymax")))

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5
# MAGIC ## Compter le nombre contrat en vigueur ("func_en_vigueur" = 1) par reseau et par région.
# MAGIC ## on affichera les resultats dans deux colonnes une colonne POL_OrigineDistrib et result qui elle contiendra la clé region (Reg_region) et la valeur du comptage:
# MAGIC  POL_OrigineDistrib | Result
# MAGIC  
# MAGIC  AGA                | {"64": 2, "65":3}

# COMMAND ----------

data_5 = [
          ["0045760152", "1", 1, Decimal(0.43), '1', "64", "AGA"],
          ["0045760152", "2", 1, Decimal(0.63), '1', "65", "BROKER"],
          ["0045760152", "3", 2, Decimal(0.53), '1', "64", "AGA"],
          ["0045760152", "4", 2, Decimal(0.73), '1', "64", "AEP"],
          ["0045760152", "5", 3, Decimal(0.43), '1', "65", "AGA"],
          ["0054760159", "6", 1, Decimal(0.33), '1', "64", "AEP"],
          ["0045210154", "7", 3, Decimal(0.73), '1', "13", "BROKER"],
          ["0045153153", "8", 1, Decimal(0.29), '1', "68", "AGA"],
          ["0085761123", "9", 2, Decimal(0.29), '1', "13", "AGA"],
          ["0076576011", "10", 1, Decimal(0.96), '1', "64", "AGA"],
          ["0045760152", "11", 1, Decimal(0.93), '1', "67", "AGA"],
          ["0045760152", "12", 1, Decimal(0.13), '1', "65", "AGA"],
          ["0045760152", "13", 2, Decimal(0.45), '1', "68", "AGA"],
          ["0045760152", "14", 2, Decimal(0.11), '1', "64", "WEB"],
          ["0045760152", "15", 3, Decimal(0.12), '1', "13", "AEP"],
          ["0054760159", "16", 1, Decimal(0.32), '1', "67", "BROKER"],
          ["0045210154", "17", 3, Decimal(0.65), '1', "13", "AGA"],
          ["0045153153", "18", 1, Decimal(0.56), '1', "83", "AXA_PART"],
          ["0085761123", "19", 2, Decimal(0.32), '1', "65", "AGA"],
          ["0045760152", "20", 1, Decimal(1.56), '1', "68", "WEB"],
          ["0045760152", "21", 1, Decimal(1.64), '1', "67", "AGA"],
          ["0045760152", "22", 2, Decimal(1.54), '1', "83", "AXA_PART"],
          ["0045760152", "23", 2, Decimal(3.22), '1', "13", "AGA"],
          ["0045760152", "24", 3, Decimal(1.25), '1', "67", "AEP"],
          ["0054760159", "25", 3, Decimal(2.24), '1', "68", "AGA"],
          ["0045210154", "26", 3, Decimal(1.64), '1', "13", "AEP"],
          ["0045153153", "27", 1, Decimal(1.44), '1', "65", "WEB"],
          ["0085761123", "28", 3, Decimal(2.13), '1', "65", "BROKER"],
          ["0045153153", "29", 1, Decimal(4.64), '1', "65", "AGA"],
          ["0045453153", "30", 1, Decimal(1.25), '1', "83", "AXA_PART"],
          ["0012313153", "31", 1, Decimal(3.25), '1', "68", "AGA"],
          ["0056123153", "32", 1, Decimal(4.12), '1', "67", "AEP"],
          ["0045112312", "33", 1, Decimal(3.21), '1', "13", "BROKER"],
          ["0045531235", "34", 1, Decimal(2.23), '1', "68", "AGA"],
          ["0076576015", "35", 1, Decimal(1.29), '1', "13", "AEP"]]

rdd_5 = sc.parallelize(data_5)
df_5 = sqlContext.createDataFrame(rdd_5, schema=['POL_NumCnt', 'id', 'Montant_cote', 'PER_ValeurCli', 'func_en_vigueur', 'REG_Region', "POL_OrigineDistrib"])

# df_5.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Filtre les contrats en vigueur
contrats_en_vigueur = df_5.filter(F.col("func_en_vigueur") == 1)
# Regrouper par POL_OrigineDistrib et REG_Region, puis compter le nombre de contrats
contrats_count = (contrats_en_vigueur
                  .groupBy("POL_OrigineDistrib", "REG_Region")
                  .count())
# Groupby des résultats par POL_OrigineDistrib et créer un dictionnaire pour chaque réseau
result_df = (contrats_count
             .groupBy("POL_OrigineDistrib")
             .agg(F.map_from_entries(
                 F.collect_list(F.struct("REG_Region", "count"))
             ).alias("Result")))
display(result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6 cree une table des sinistres definie comme suit :
# MAGIC   * definition du dataframe d'input
# MAGIC     *   POL_NumCnt numero de contrat
# MAGIC     *   CLA_Num_sinX_CP numero de sinistre  (X de 1 a 10)
# MAGIC     *   CLA_DtSurv_sinX_CP date de survenance du sinistre (X de 1 a 10)
# MAGIC     *  CLA_Etat_sinX_CP etat du sinistre (X de 1 a 10)
# MAGIC     *   CLA_TopCorpMat_sinX_CP nature du sinistre (X de 1 a 10)
# MAGIC   * definition du dataframe de sortie :
# MAGIC     *   POL_NumCnt
# MAGIC     *   num_sin
# MAGIC     *   dt_sin
# MAGIC     *   etat_sin
# MAGIC     *   nature_sin
# MAGIC   * indication : on supprimera les sinistres null

# COMMAND ----------

df_sin_in = spark.read.parquet('file:/tmp/TP_unzipped/TP/data_input_unzipped/sinistre_input_test.parquet')

df_sin_in.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Liste des indices pour les sinistres
sinistres = list(range(1, 11))

# Création d'une liste pour stocker les DataFrames intermédiaires
dfs = []

# Transformation de chaq sinistre en une structure simplifiée (POL_NumCnt, num_sin, dt_sin, etat_sin, nature_sin)
for i in sinistres:
    # Vérification et sélection des colonnes dispo pour chaq sinistre
    df_sin_tmp = (df_sin_in
                  .select(
                      F.col("POL_NumCnt"),
                      F.col(f"CLA_Num_sin{i}_CP").alias("num_sin"),
                      F.col(f"CLA_DtSurv_sin{i}_CP").alias("dt_sin"),
                      F.col(f"CLA_Etat_sin{i}_CP").alias("etat_sin"),
                      F.col(f"CLA_TopCorpMat_sin{i}_CP").alias("nature_sin") 
                  )
                  .filter(F.col("num_sin").isNotNull()))  # Supp les sinistres avec num_sin null
    dfs.append(df_sin_tmp)

# Union de tous les DataFrames intermédiaires pour créer le df final
df_sin_out = dfs[0]
for df in dfs[1:]:
    df_sin_out = df_sin_out.union(df)

display(df_sin_out)

