import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from prep import top_paid_artists, top_free_artists, top_free_songs, top_paid_songs
import prep
import streamlit as st
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import pandas as pd
import altair as alt



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

file_path = '/Users/isiah/Downloads/Data/listen_events'
df_listen = None

try:
    spark = create_spark_session()
    df_listen_raw = spark.read.json(file_path)
    print('Data loaded successfully from listen_events.')

    fix_encoding_udf = udf(fix_multiple_encoding, StringType())

    df_fixed = df_listen_raw.withColumn("artist", fix_encoding_udf(col("artist"))) \
                             .withColumn("song", fix_encoding_udf(col("song")))
    df_listen = df_fixed.select('userId', 'lastName', 'firstName', 'gender', 'song', 'artist', 'duration', 'sessionId', 'itemInSession', 'auth', 'level', 'city', 'state', 'zip', 'lat', 'lon', 'registration', 'userAgent', 'ts')
    df_listen = df_listen.withColumnRenamed("level", "subscription")

except Exception as e:
    print(f'Error loading data: {e}')
    st.error("Failed to load data. Please check the console for errors.")
    st.stop()



# Display top paid artists
paid_artists = prep.top_paid_artists(df=df_listen, paid_status='paid')
st.title("Top Artists and Songs Analysis")

# Top Paid Artists
if paid_artists:
    st.subheader("Horizontal Bar Chart of Top Artists for Paid Users")
    paid_artists_df = paid_artists.toPandas()
    paid_artists_df = paid_artists_df.sort_values(by='count', ascending=True)

    chart_paid_artists = alt.Chart(paid_artists_df).mark_bar().encode(
        x=alt.X('count:Q', title='Count'),
        y=alt.Y('artist:N', sort='-x', title='Artist'),
        tooltip=['artist', 'count']
    ).properties(
        width=700,
        height=400
    )
    st.altair_chart(chart_paid_artists, use_container_width=True)
else:
    st.write("No data available for paid users.")

# Top Free Artists
free_artists = prep.top_free_artists(df=df_listen, free_status='free')
if free_artists:
    st.subheader("Horizontal Bar Chart of Top Artists for Free Users")
    free_artists_df = free_artists.toPandas()
    free_artists_df = free_artists_df.sort_values(by='count', ascending=True)

    chart_free_artists = alt.Chart(free_artists_df).mark_bar().encode(
        x=alt.X('count:Q', title='Count'),
        y=alt.Y('artist:N', sort='-x', title='Artist'),
        tooltip=['artist', 'count']
    ).properties(
        width=700,
        height=400
    )
    st.altair_chart(chart_free_artists, use_container_width=True)
else:
    st.write("No data available for free users.")

# Top Paid Songs
paid_songs = prep.top_paid_songs(df=df_listen, paid_status='paid')
if paid_songs:
    st.subheader("Horizontal Bar Chart of Top Songs for Paid Users")
    paid_songs_df = paid_songs.toPandas()
    paid_songs_df = paid_songs_df.sort_values(by='count', ascending=True)

    chart_paid_songs = alt.Chart(paid_songs_df).mark_bar().encode(
        x=alt.X('count:Q', title='Count'),
        y=alt.Y('song:N', sort='-x', title='Song'),
        tooltip=['song', 'count']
    ).properties(
        width=700,
        height=400
    )
    st.altair_chart(chart_paid_songs, use_container_width=True)
else:
    st.write("No data available for paid songs.")

# Top Free Songs
free_songs = prep.top_free_songs(df=df_listen, free_status='free')
if free_songs:
    st.subheader("Horizontal Bar Chart of Top Songs for Free Users")
    free_songs_df = free_songs.toPandas()
    free_songs_df = free_songs_df.sort_values(by='count', ascending=True)

    chart_free_songs = alt.Chart(free_songs_df).mark_bar().encode(
        x=alt.X('count:Q', title='Count'),
        y=alt.Y('song:N', sort='-x', title='Song'),
        tooltip=['song', 'count']
    ).properties(
        width=700,
        height=400
    )
    st.altair_chart(chart_free_songs, use_container_width=True)
else:
    st.write("No data available for free songs.")




