import pandas as pd
import pyspark
from pyspark.sql.functions import desc, asc, count, col, when, to_timestamp, year, month, date_format, sum, when, udf, from_unixtime
from pyspark.sql.types import StringType


############
# Kunle
############

def get_artist_state_listen( df: pyspark.sql.dataframe.DataFrame , artist: str) -> pyspark.sql.dataframe.DataFrame:
    '''
    Filters and aggregates a pyspark dataframe to count listens by artist and state

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe
        artist (str): name of the artist

    Returns:
        filtered and aggregated dataframe 
    
    '''
    df = df.groupBy('artist','state').agg(count('*').alias('listens')).where(col('artist') == artist).orderBy(desc('listens'))
    return df

def get_artist_over(df: pyspark.sql.dataframe.DataFrame, number_of_lis: int) -> list:
    '''
    Takes in a pyspark dataframe and returns list of artists with at least a states number of listens

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe
        number_of_lis (int): min number of listens

    Returns:
        list: number of artists with at least the specified number of listens

    '''
    df = df.groupBy('artist').agg(count('*').alias('listens')).filter(col('listens') >= number_of_lis).orderBy(desc('listens'))
    df_list = [data[0] for data in df.select('artist').collect()]
    return df_list

def map_prep_df(df: pyspark.sql.dataframe.DataFrame) -> pd.core.frame.DataFrame:
    '''
    Takes a filtered pyspark dataframe and returns a pandas dataframe with state names 

    Arg:
        df (pyspark.sql.dataframe.DataFrame): dataframe

    Returns:
        pandas dataframe: dataframe of artist, # of listens, US states: name & abr

    '''
    us_state_to_abbrev = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
    "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN",
    "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
    }
    
    us_states = list(us_state_to_abbrev.items())
    us_states_columns = ['NAME', 'state']  # <-- make sure this matches
    states_df = pd.DataFrame(us_states, columns=us_states_columns)

    df = df.toPandas()
    artist = df.artist[0]

    map_prep = pd.merge(
    left = df,
    right = states_df,
    left_on = 'state',
    right_on ='state',
    how = 'right')

    map_prep.listens = map_prep.listens.fillna(0)
    map_prep.artist = map_prep.artist.fillna(artist)
    
    return map_prep


def top_5(df: pyspark.sql.dataframe.DataFrame) ->  pyspark.sql.dataframe.DataFrame:
    df = df.orderBy(desc('listens')).limit(5)
    return df

############
# angel
############
def get_user_list(df: pyspark.sql.dataframe.DataFrame, state: str) -> pd.core.frame.DataFrame:
     
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
    if state == 'Nationwide':
        updated_listening_duration
    else:
        updated_listening_duration = updated_listening_duration.filter(col("state").isin(state))
    
    
    # Group by year, month, subscription, and month_name, then sum the durations
    duration_grouped = updated_listening_duration.groupBy("year", "month", "month_name", "subscription") \
            .agg(sum("duration").alias("total_duration")) \
            .orderBy("year", "month", "subscription")
    
    #convert to a pandas dataframe
    updated_listening_duration_pd = duration_grouped.toPandas()

    return updated_listening_duration_pd



############
# James
############

def get_top_10_artists(df: pyspark.sql.dataframe.DataFrame , state: str) -> pd.core.frame.DataFrame:
    """
    Finds the top 10 artists, ordered by play count.

    Args:
        dataframe: An optional PySpark DataFrame. Defaults to the globally defined df_listen.
        selected_state: An optional string representing the state to filter by.
                        If None (default), it aggregates across all states.

    Returns:
        A PySpark DataFrame containing the top 10 artists and their counts.
    """
    # if df is None:
    #     print("Warning: df_listen is None. Ensure data loading was successful.")
    #     return None

    if state == 'Nationwide':
        #title = "Top 10 National Artists"
        filtered_df = df
    else:
        #title = f"Top 10 Artists in {selected_state}"
        filtered_df = df.filter(col("state") == state)
    
        

    top_10_artists_df = filtered_df.groupBy("artist") \
                                   .agg(count("*").alias("count")) \
                                   .orderBy(desc("count")) \
                                   .limit(10) 

    #print(title + ":")
    return top_10_artists_df.toPandas()


def clean(df: pyspark.sql.dataframe.DataFrame) ->  pyspark.sql.dataframe.DataFrame:
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