import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, count, desc
from pyspark.sql.types import StringType
import altair as alt
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

    # Now, subsequent functions using df_listen will use the fixed data
    # Example of a later function:
    df_listen.groupBy("artist").count().orderBy(col("count").desc()).show()

except Exception as e:
    print(f'Error loading data: {e}')
    df_listen = None


df_listen = df_listen.select('userId', 'lastName', 'firstName', 'gender', 'song', 'artist', 'duration', 'sessionId', 'itemInSession', 'auth', 'level', 'city', 'state', 'zip', 'lat', 'lon', 'registration', 'userAgent', 'ts')
df_listen = df_listen.withColumnRenamed("level", "subscription")


# Convert milliseconds to seconds
df_listen = df_listen.withColumn("ts_seconds", col("ts") / 1000)
# Convert the Unix timestamp (in seconds) to a readable timestamp format
readable_ts = from_unixtime(col("ts_seconds"), "yyyy-MM-dd HH:mm:ss")
# Replace the original 'ts' column with the readable timestamp
df_listen = df_listen.withColumn("ts", readable_ts).drop("ts_seconds")
# Show the modified DataFrame
df_listen.show(truncate=False)

def get_top_10_artists(selected_state=None, dataframe=df_listen):
    """
    Finds the top 10 artists, defaulting to the globally defined df_listen
    if no dataframe is explicitly provided.

    Args:
        dataframe: An optional PySpark DataFrame. Defaults to the globally defined df_listen.
        selected_state: An optional string representing the state to filter by.
                        If None (default), it aggregates across all states.

    Returns:
        A PySpark DataFrame containing the top 10 artists and their counts.
    """
    if selected_state:
        title = f"Top 10 Artists in {selected_state}"
        filtered_df = dataframe.filter(col("state") == selected_state)
    else:
        title = "Top 10 National Artists"
        filtered_df = dataframe

    top_10_artists_df = filtered_df.groupBy("artist") \
                                   .agg(count("*").alias("count")) \
                                   .orderBy(desc("count")) \
                                   .limit(10) \
                                   .select(col("artist"), col("count"))

    print(title + ":")
    top_10_artists_df.show()
    return top_10_artists_df

def create_subscription_pie_chart(selected_state=None, free_color='red', paid_color='green', dataframe=df_listen):
    """
    Generates an Altair pie chart showing the distribution of free vs. paid
    subscriptions. Defaults to the national distribution using the globally
    defined df_listen if no dataframe is explicitly provided.

    Args:
        dataframe: An optional PySpark DataFrame. Defaults to the globally defined df_listen.
        selected_state: An optional string representing the state to filter by.
                        If None (default), it aggregates across all states.
        free_color: The color to use for 'free' subscriptions (default: 'red').
        paid_color: The color to use for 'paid' subscriptions (default: 'green').

    Returns:
        An Altair chart object.
    """
    if selected_state:
        # Visualize for a specific state
        title = f"Subscription Type Distribution in {selected_state}"
        filtered_df = dataframe.filter(col("state") == selected_state)
    else:
        # Visualize for all states (national)
        title = "National Subscription Type Distribution"
        filtered_df = dataframe

    free_vs_paid_df_spark = filtered_df.groupBy("subscription") \
                                   .agg(count("*").alias("count")) \
                                   .orderBy(desc("count")) \
                                   .select(col("subscription"), col("count"))

    pandas_df = free_vs_paid_df_spark.toPandas()

    chart = alt.Chart(pandas_df).mark_arc().encode(
        theta=alt.Theta(field="count", type="quantitative"),
        color=alt.Color(field="subscription", type="nominal",
                        scale=alt.Scale(domain=['free', 'paid'],
                                        range=[free_color, paid_color]),
                        legend=alt.Legend(title="Subscription Type", orient="bottom")),
        order=alt.Order(field="count", sort="descending"),
        tooltip=["subscription", "count"]
    ).properties(
        title=title
    )
    return chart

#Streamlit Titling
st.title("Muse Dash")
# st.subheader("Testing")

#Sidebar
st.sidebar.header("Select a State")
available_states = df_listen.select("state").distinct().orderBy("state").rdd.flatMap(lambda x: x).collect()
selected_state = st.sidebar.selectbox("Filter by State (Optional):", ["All"] + available_states)

col_table = st.columns((5, 1, 5), gap='medium')

with col_table[0]:
    #Top 10 Artists
    st.header("Top 10 Artists")
    if selected_state == "All":
        top_artists_df_spark = get_top_10_artists(dataframe=df_listen)
        if top_artists_df_spark is not None:
            top_artists_df_pandas = top_artists_df_spark.toPandas()
            st.table(top_artists_df_pandas)

    else:
        top_artists_df_spark = get_top_10_artists(selected_state=selected_state, dataframe=df_listen)
        if top_artists_df_spark is not None:
            top_artists_df_pandas = top_artists_df_spark.toPandas()
            st.table(top_artists_df_pandas)
    
    st.header("What if I add more things here")

with col_table[2]:
    # --- Display Subscription Pie Chart ---
    # st.subheader("Subscription Type Distribution")
    if selected_state == "All":
        subscription_chart = create_subscription_pie_chart(dataframe=df_listen)
        st.altair_chart(subscription_chart, use_container_width=True)
    else:
        subscription_chart_state = create_subscription_pie_chart(selected_state=selected_state, dataframe=df_listen)
        st.altair_chart(subscription_chart_state, use_container_width=True)
    
    st.header("What if I add more things here")

with col_table[1]:
    st.empty()
    st.write('')
