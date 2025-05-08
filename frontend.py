import streamlit as st
import numpy as np
import plotly.express as px
import engine
import plotly.graph_objects as go



from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Museh PySpark Learning") \
        .getOrCreate()

## Verify that SparkSession is created

try:
    df_listen = spark.read.json ('./Data/listen_events')
    print('Data loaded successfully')
except Exception as e:
    print(f'Error loading data: {e}')


cleaned_listen = engine.clean(df=df_listen)


# Sidebar
st.sidebar.header("Select a State")
available_states = cleaned_listen.select("state").distinct().orderBy("state").rdd.flatMap(lambda x: x).collect()
selected_state = st.sidebar.selectbox("Filter by State (Optional):", ["All"] + available_states)

top_10 = engine.get_top_10_artists(df=cleaned_listen, selected_state=selected_state)
st.table(top_10)