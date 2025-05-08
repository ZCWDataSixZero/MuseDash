import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, count, desc
from pyspark.sql.types import StringType
import altair as alt

# Initialize SparkSession
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

# Load data and perform initial transformations
file_path = './Data/listen_events'
df_listen = None  # Initialize outside the try block

try:
    spark = create_spark_session()  # Use the function to get the session
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

def get_top_10_artists(selected_state=None, dataframe=df_listen):
    """
    Finds the top 10 artists, ordered by play count.

    Args:
        dataframe: An optional PySpark DataFrame. Defaults to the globally defined df_listen.
        selected_state: An optional string representing the state to filter by.
                        If None (default), it aggregates across all states.

    Returns:
        A PySpark DataFrame containing the top 10 artists and their counts.
    """
    if dataframe is None:
        print("Warning: df_listen is None. Ensure data loading was successful.")
        return None

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
    return top_10_artists_df

def create_subscription_pie_chart(selected_state=None, free_color='red', paid_color='green', dataframe=df_listen):
    """
    Generates an Altair pie chart showing the distribution of free vs. paid
    subscriptions. Defaults to the national distribution using the provided dataframe.

    Args:
        dataframe: A PySpark DataFrame.
        selected_state: An optional string representing the state to filter by.
                        If None (default), it aggregates across all states.
        free_color: The color to use for 'free' subscriptions (default: 'red').
        paid_color: The color to use for 'paid' subscriptions (default: 'green').

    Returns:
        An Altair chart object.
    """
    if dataframe is None:
        print("Warning: df_listen is None. Ensure data loading was successful.")
        return None

    if selected_state:
        title = f"Subscription Type Distribution in {selected_state}"
        filtered_df = dataframe.filter(col("state") == selected_state)
    else:
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
    ).configure_view(
        fillOpacity=0  # Make the chart background transparent
    )
    return chart

if __name__ == "__main__":
    # Example of how you might use the functions outside of Streamlit
    spark_session = create_spark_session()
    if df_listen is not None:
        top_artists = get_top_10_artists(dataframe=df_listen)
        if top_artists is not None:
            top_artists.show()

        national_subscription_chart = create_subscription_pie_chart(dataframe=df_listen)
        if national_subscription_chart is not None:
            print("\nNational Subscription Pie Chart (Altair object):")
            print(national_subscription_chart.to_json())