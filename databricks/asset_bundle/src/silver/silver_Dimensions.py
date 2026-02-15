# Databricks notebook source


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import os
import sys

project_pth = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_pth)

from utils.transformation import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUTOLOADER

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimUser

# COMMAND ----------

df_user = spark.readStream.format('cloudfiles')\
    .option('cloudfiles.format', 'parquet')\
    .option('cloudfiles.schemaLocation', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimUser/checkpoint')\
    .load('abfss://bronze@spotifystorageakiel.dfs.core.windows.net/DimUser')

# COMMAND ----------

# Transformation 1
df_user_transformed = df_user.withColumn('user_name', upper(col('user_name')))

# Transformation 2
df_user_obj = reusable()
df_user_transformed = df_user_obj.dropColumns(df_user_transformed, ['_rescued_data'])

# Transformation 3
df_user_transformed = df_user_transformed.dropDuplicates(['user_id'])


# COMMAND ----------

# Write stream
query = df_user_transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", 
            "abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimUser/stream_checkpoint") \
    .trigger(once=True) \
    .option('path', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimUser/data')\
    .toTable('spotify_cata.silver.DimUser')
    
query.awaitTermination()

# COMMAND ----------

df_result = spark.read.format("delta").load("abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimUser/data")
display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

df_artist = spark.readStream.format('cloudfiles')\
    .option('cloudfiles.format', 'parquet')\
    .option('cloudfiles.schemaLocation', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimArtist/checkpoint')\
    .load('abfss://bronze@spotifystorageakiel.dfs.core.windows.net/DimArtist')

# COMMAND ----------

# Transformation 1
df_art_obj = reusable()

df_artist = df_art_obj.dropColumns(df_artist, ['_rescued_data'])
df_artist = df_artist.dropDuplicates(['artist_id'])

# COMMAND ----------

# Write stream
query = df_artist.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", 
            "abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimArtist/stream_checkpoint") \
    .trigger(once=True) \
    .option('path', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimArtist/data')\
    .toTable('spotify_cata.silver.DimArtist')

query.awaitTermination()

# COMMAND ----------

df_art = spark.read.format("delta").load("abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimArtist/data")
display(df_art)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack

# COMMAND ----------

df_track = spark.readStream.format('cloudfiles')\
    .option('cloudfiles.format', 'parquet')\
    .option('cloudfiles.schemaLocation', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimTrack/checkpoint')\
    .load('abfss://bronze@spotifystorageakiel.dfs.core.windows.net/DimTrack')

# COMMAND ----------

# Transformation 1
df_track = df_track.withColumn('durationFlag', when(col('duration_sec')<150, 'low')\
                                            .when(col('duration_sec')<300, 'medium')\
                                            .otherwise('high'))

# Transformation 2
df_track = df_track.withColumn('track_name', regexp_replace(col('track_name'), '-', ' '))

# Transformation 3
df_track_obj = reusable()

df_track = df_track_obj.dropColumns(df_track, ['_rescued_data'])
df_track = df_track.dropDuplicates(['track_id'])

# COMMAND ----------

# Write stream
query = df_track.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", 
            "abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimTrack/stream_checkpoint") \
    .trigger(once=True) \
    .option('path', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimTrack/data')\
    .toTable('spotify_cata.silver.DimTrack')

query.awaitTermination()

# COMMAND ----------

df_tra = spark.read.format("delta").load("abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimTrack/data")
display(df_tra)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate

# COMMAND ----------

df_date = spark.readStream.format('cloudfiles')\
    .option('cloudfiles.format', 'parquet')\
    .option('cloudfiles.schemaLocation', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimDate/checkpoint')\
    .load('abfss://bronze@spotifystorageakiel.dfs.core.windows.net/DimDate')

# COMMAND ----------

# Transformation 1
df_date_obj = reusable()
df_date = df_date_obj.dropColumns(df_date, ['_rescued_data'])

# COMMAND ----------

# Write stream
query = df_date.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", 
            "abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimDate/stream_checkpoint") \
    .trigger(once=True) \
    .option('path', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimDate/data')\
    .toTable('spotify_cata.silver.DimDate')

query.awaitTermination()

# COMMAND ----------

df_dt = spark.read.format("delta").load("abfss://silver@spotifystorageakiel.dfs.core.windows.net/DimDate/data")
display(df_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream

# COMMAND ----------

df_fact = spark.readStream.format('cloudfiles')\
    .option('cloudfiles.format', 'parquet')\
    .option('cloudfiles.schemaLocation', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/FactStream/checkpoint')\
    .load('abfss://bronze@spotifystorageakiel.dfs.core.windows.net/FactStream')

# COMMAND ----------

#Trandformation
df_fact = reusable().dropColumns(df_fact, ['_rescued_data'])

# COMMAND ----------

# Write stream
query = df_fact.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", 
            "abfss://silver@spotifystorageakiel.dfs.core.windows.net/FactStream/stream_checkpoint") \
    .trigger(once=True) \
    .option('path', 'abfss://silver@spotifystorageakiel.dfs.core.windows.net/FactStream/data')\
    .toTable('spotify_cata.silver.FactStream')

query.awaitTermination()