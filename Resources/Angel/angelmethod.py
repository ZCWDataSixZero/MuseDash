import pandas as pd
import pyspark
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, when, to_timestamp, year, month, date_format, sum, avg, when, udf, from_unixtime, countDistinct



def calculate_kpis(df: pyspark.sql.dataframe.DataFrame):
    """
    Calculates total users and average listening time from a PySpark DataFrame.

    Args:
        df: A PySpark DataFrame with 'user_id' and 'duration_seconds' columns.

    Returns:
        A tuple containing (total_users, average_listening_time).
    """
    total_users = df.select(col("userId")).distinct().count()
    average_listening_time = df.select(avg("duration")).collect()[0][0]
    total_duration_sum = df.filter(df["level"] == "paid").agg(sum("duration")).collect()[0][0]
    return total_users, average_listening_time, total_duration_sum


def get_user_list(df: pyspark.sql.dataframe.DataFrame, selected_states = None) -> pd.core.frame.DataFrame:
    '''
    Goes through every instance of paid users and changes 'free' into paid. This ensures free users aren't being overcounted. 

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe
        selected_states: names of states

    Returns:
        filtered and aggregated dataframe
    ''' 
    
    
    paid_users = (
            df.filter(col("level") == "paid")
            .select("userId")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
    
      #Update subscription of free to paid users from 'free' to 'paid'
    updated_listening_duration = df.withColumn(
            "subscription",
            when(col("userId").isin(paid_users), "paid").otherwise(col("subscription"))
        )
     
            # Filter data on selected states
    if selected_states:
        updated_listening_duration = updated_listening_duration.filter(col("state").isin(selected_states))
    else:
        updated_listening_duration
    
    # Group by year, month, subscription, and month_name, then sum the durations
    duration_grouped = updated_listening_duration.groupBy("year", "month", "month_name", "subscription") \
            .agg(sum("duration").alias("total_duration")) \
            .orderBy("year", "month", "subscription")
    
    #convert to a pandas dataframe
    updated_listening_duration_pd = duration_grouped.toPandas()

    return updated_listening_duration_pd

def fix_multiple_encoding(text):
    '''
    
    Attempts to fix multiple layers of incorrect encoding.

    '''
   
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


def clean(df: pyspark.sql.dataframe.DataFrame) ->  pyspark.sql.dataframe.DataFrame:
    '''
        Cleans the dataframe -- fixes encoding issue, adding filtering needed columns, turning 'ts' column into a readable timestamp

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe
        

    Returns:
        a cleaned pyspark dataframe ready for visualizing
    
    '''


    fix_encoding_udf = udf(fix_multiple_encoding, StringType())
    df = df.withColumn("artist", fix_encoding_udf(col("artist"))) \
                         .withColumn("song", fix_encoding_udf(col("song")))
    
    df = df.selectExpr('userId', 'lastName', 'firstName', 'gender', 'song', 'artist', \
                  'duration', 'sessionId', 'itemInSession', 'auth', 'level as subscription',\
                      'city', 'state', 'zip', 'lat', 'lon', 'registration', 'userAgent', 'ts')

    df = df.withColumn("ts", to_timestamp(col("ts").cast("long") / 1000))
    df = df.withColumn("year", year(col("ts"))) \
            .withColumn("month", month(col("ts"))) \
            .withColumn("month_name", date_format(col("ts"), "MMMM"))

    return df



