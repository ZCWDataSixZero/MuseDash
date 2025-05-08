import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from prep import top_paid_artists
import prep
import streamlit as st
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import pandas as pd



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

file_path = '/Users/isiah/Projects/P1/MuseDash/Resources/Isiah/Data'
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



# spark = SparkSession.builder.appName("Musedash Streamlit").getOrCreate()

# try:
#     df_listen = spark.read.json ('./Data/listen_events')
#     print('Data loaded successfully')
# except Exception as e:
#     print(f'Error loading data: {e}')


# get artists

paid_artists = prep.top_paid_artists(df=df_listen, paid_status='paid')

st.title("Top Artists For Paid Users")

# Create horizontal bar chart
if paid_artists:
    st.subheader("Horizontal Bar Chart of Top Artists of Paid Users")
    # Convert to Pandas DataFrame for Streamlit
    paid_artists_df = paid_artists.toPandas()
    paid_artists_df = paid_artists_df.sort_values(by='count', ascending=False)
    st.bar_chart(paid_artists_df.set_index('artist')['count'], use_container_width=True)
else:
    st.write("No data available for paid users.")

    


