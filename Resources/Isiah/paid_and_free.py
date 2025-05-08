import pandas as pd
from pyspark.sql import SparkSession, col
from pyspark.sql.functions import from_unixtime, count, udf, desc
from pyspark.sql.types import StringType
import matplotlib as plt
import streamlit as st


## Initialize the SparkSession
# appName is the name of the application
# getOrCreate() creates a new session or retrieves an existing one
# Initialize SparkSession
@st.cache_resource
def create_spark_session():
    return SparkSession.builder.appName("Streamlit PySpark").getOrCreate()

def fix_multiple_encoding(text):
    """Attempts to fix multiple layers of incorrect encoding."""
    if text is None:
        return None
    original_text = text
    try:
        decoded_once = text.encode('latin-1').decode('utf-8', errors='replace')
        if decoded_once != original_text and '?' not in decoded_once:
            decoded_twice = decoded_once.encode('latin-1').decode('utf-8', errors='replace')
            if decoded_twice != decoded_once and '?' not in decoded_twice:
                return decoded_twice
            return decoded_once
    except UnicodeEncodeError:
        pass
    except UnicodeDecodeError:
        pass
    return original_text



file_path = './Data/listen_events'

try:
    spark = SparkSession.builder.appName("ReassignDF").getOrCreate()
    df_listen = spark.read.json(file_path)
    print('Data loaded successfully from listen_events.')

    fix_encoding_udf = udf(fix_multiple_encoding, StringType())

    df_fixed = df_listen.withColumn("artist", fix_encoding_udf(col("artist"))) \
                         .withColumn("song", fix_encoding_udf(col("song")))

    print('\nData after attempting to fix encoding in artist and song:')
    df_fixed.show(10, truncate=False)

    # Reassign df_listen to point to the fixed DataFrame
    df_listen = df_fixed



except Exception as e:
    print(f'Error loading data: {e}')
    df_listen = None


#rename colulmn 
df_listen = df_listen.withColumnRenamed('level', 'subscription')   

#select relevant column
df_listen = df_listen.select('userId', 'artist', 'song', 'subscription', 'city', 'state')



def top_songs_paid(selected_subscription=None, dataframe=df_listen):
    """
    Shows top songs across userbase
    """
