import pandas as pd
import pyspark
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, year, month, date_format, sum, when

def get_user_list(df: pyspark.sql.dataframe.DataFrame, selected_states = None) -> pd.core.frame.DataFrame:
     
     # Find the paid users
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
    duration_grouped = updated_listening_duration.groupBy("year", "month", "month_name", "level") \
            .agg(sum("duration").alias("total_duration")) \
            .orderBy("year", "month", "level")
    
    #convert to a pandas dataframe
    updated_listening_duration_pd = duration_grouped.toPandas()

    return updated_listening_duration_pd

    