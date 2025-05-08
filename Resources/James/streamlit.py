import streamlit as st
import pandas as pd
import altair as alt
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, count, desc
from pyspark.sql.types import StringType
from muse import get_top_10_artists, create_subscription_pie_chart

def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

local_css("style.css")

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
df_listen = None

try:
    spark = create_spark_session()
    df_listen_raw = spark.read.json(file_path)
    print('Data loaded successfully from listen_events.')

    fix_encoding_udf = udf(fix_multiple_encoding, StringType())

    df_fixed = df_listen_raw.withColumn("artist", fix_encoding_udf(col("artist"))) \
                             .withColumn("song", fix_encoding_udf(col("song")))

    print('\nData after attempting to fix encoding in artist and song:')
    df_fixed.show(10, truncate=False)

    df_listen = df_fixed.select('userId', 'lastName', 'firstName', 'gender', 'song', 'artist', 'duration', 'sessionId', 'itemInSession', 'auth', 'level', 'city', 'state', 'zip', 'lat', 'lon', 'registration', 'userAgent', 'ts')
    df_listen = df_listen.withColumnRenamed("level", "subscription")

    # Convert milliseconds to seconds
    df_listen = df_listen.withColumn("ts_seconds", col("ts") / 1000)
    # Convert the Unix timestamp (in seconds) to a readable timestamp format
    readable_ts = from_unixtime(col("ts_seconds"), "yyyy-MM-dd HH:mm:ss")
    # Replace the original 'ts' column with the readable timestamp
    df_listen = df_listen.withColumn("ts", readable_ts).drop("ts_seconds")
    print('\nTransformed DataFrame:')
    df_listen.show(truncate=False)

except Exception as e:
    print(f'Error loading data: {e}')
    st.error("Failed to load data. Please check the console for errors.")
    st.stop()

# Streamlit Titling
st.title("Muse Dash")

# Sidebar
st.sidebar.header("Select a State")
available_states = df_listen.select("state").distinct().orderBy("state").rdd.flatMap(lambda x: x).collect()
selected_state = st.sidebar.selectbox("Filter by State (Optional):", ["All"] + available_states)

col_table = st.columns((5, 1, 5), gap='medium')

with col_table[0]:
    # Top 10 Artists
    if selected_state == "All":
        st.header("Top 10 National Artists")
        top_artists_df_spark = get_top_10_artists(dataframe=df_listen)
        if top_artists_df_spark is not None:
            top_artists_df_pandas = top_artists_df_spark.toPandas()
            # Add a 1-based index
            top_artists_df_pandas.index = range(1, len(top_artists_df_pandas) + 1)
            st.table(top_artists_df_pandas)
    else:
        st.header(f"Top 10 Artists in {selected_state}")
        top_artists_df_spark = get_top_10_artists(selected_state=selected_state, dataframe=df_listen)
        if top_artists_df_spark is not None:
            top_artists_df_pandas = top_artists_df_spark.toPandas()
            # Add a 1-based index
            top_artists_df_pandas.index = range(1, len(top_artists_df_pandas) + 1)
            st.table(top_artists_df_pandas)

    st.header("What if I add more things here")

with col_table[2]:
    # --- Display Subscription Pie Chart ---
    if selected_state == "All":
        subscription_chart = create_subscription_pie_chart(dataframe=df_listen, free_color="#E7187C", paid_color="#5AE718")
        st.altair_chart(subscription_chart, use_container_width=True)
    else:
        subscription_chart_state = create_subscription_pie_chart(selected_state=selected_state, dataframe=df_listen, free_color="#E7187C", paid_color="#5AE718")
        st.altair_chart(subscription_chart_state, use_container_width=True)

    st.header("What if I add more things here")

with col_table[1]:
    st.empty()
    st.write('')