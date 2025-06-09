import pandas as pd
import pyspark
from pyspark.sql.functions import desc, asc, avg, count, col, when, to_timestamp, year, month, date_format, sum, when, udf, from_unixtime
from pyspark.sql.types import StringType
import requests
import json
from apikey import API_KEY

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

def get_artist_state( df: pyspark.sql.dataframe.DataFrame , artist: str) -> pd.core.frame.DataFrame:
    '''
    Filters and aggregates a pyspark dataframe to count listens by artist and state

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe
        artist (str): name of the artist

    Returns:
        filtered and aggregated dataframe 
    
    '''
    df = df.groupBy('artist','state').agg(count('*').alias('listens')).where(col('artist') == artist).orderBy(desc('listens'))
    return df.toPandas()

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


def get_top_50(df: pyspark.sql.dataframe.DataFrame , state: str) -> list:
    """
    Finds the top 50 artists, ordered by play count.

    Args:
        dataframe: An optional PySpark DataFrame. Defaults to the globally defined df_listen.
        selected_state: An optional string representing the state to filter by.
                        If None (default), it aggregates across all states.

    Returns:
        A PySpark DataFrame containing the top 10 artists and their counts.
    """

    if state == 'Nationwide':
        filtered_df = df
    else:
        #title = f"Top 10 Artists in {selected_state}"
        filtered_df = df.filter(col("state") == state)
    
        

    top_50_artists_df = filtered_df.groupBy("artist") \
                                   .agg(count("*").alias("Total Streams")) \
                                   .orderBy(desc("Total Streams")) \
                                   .limit(50) 
    
    top_50_artists_df = top_50_artists_df.toPandas().sort_values(by='Total Streams', ascending=False)

    
    return top_50_artists_df.artist.tolist()

def gen_genre_ai(artist_list: list) -> str:
    try:
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": API_KEY,
            "Content-Type": "application/json"
        }
        data = {
            "model": "google/gemma-3n-e4b-it:free",
            "prompt": f'Given this list of artists {artist_list}, could you tell me the 3 most popular genre of music in 30 words or less',
            "max_tokens": 2000
        }
    except requests.exceptions.RequestException as e: # Catch specific request exceptions
        # More descriptive error handling for API issues
        if response is not None:
            return f"Failed to generate genre summary (HTTP {response.status_code}): {response.text} - {str(e)}"
        else:
            return f"Failed to generate genre summary: {str(e)}"
    except Exception as e:
        # Catch any other unexpected errors
        return f"An unexpected error occurred: {str(e)}"

    response = requests.post(url, headers=headers, json=data)
    rrr = response.json()

    return rrr['choices'][0]['text']



############
# angel
############

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
    total_duration_sum = df.filter(df["subscription"] == "paid").agg(sum("duration")).collect()[0][0]
    return total_users, average_listening_time, total_duration_sum

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
            .agg((sum(col("duration")) / 60).alias("total_duration")) \
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
                                   .agg(count("*").alias("Total Streams")) \
                                   .orderBy(desc("Total Streams")) \
                                   .limit(10) 
    top_10_artists_df = top_10_artists_df.withColumnRenamed("artist", "Artist")
    
    top_10_artists_df = top_10_artists_df.toPandas().sort_values(by='Total Streams', ascending=False)

    #print(title + ":")
    return top_10_artists_df

def create_subscription_pie_chart(df: pyspark.sql.dataframe.DataFrame , state: str) -> pd.core.frame.DataFrame:
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
    # if df is None:
    #     print("Warning: df_listen is None. Ensure data loading was successful.")
    #     return None

    if state == 'Nationwide':
        #title = "National Subscription Type Distribution"
        filtered_df = df
    else:
        #title = f"Subscription Type Distribution in {selected_state}"
        filtered_df = df.filter(col("state") == state)
    
    free_vs_paid_df_spark = filtered_df.groupBy("subscription") \
                                .agg(count("*").alias("count")) \
                                .orderBy(desc("count")) 

    return free_vs_paid_df_spark.toPandas()


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

def get_states_list(df: pyspark.sql.dataframe.DataFrame) -> list:
    '''
    Takes in a pyspark dataframe and returns list of states

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe

    Returns:
        list: list of stats in the dataframe

    '''
    states_list = df.select("state").distinct().orderBy("state").rdd.flatMap(lambda x: x).collect()
    return states_list

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

####
# AI testing
####
import requests

def generate_genre_summary(artists: list[str], auth_token: str) -> str:
    prompt = f"""You are a music genre analyst. Analyze the following list of top 10 artists and generate a 2–3 sentence description that summarizes their dominant musical styles and how they might appeal to listeners. The artists are: {', '.join(artists)}."""

    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    json_data = {
        # Use the OpenRouter model name for the free DeepSeek-R1 model
        "model": "deepseek/deepseek-r1:free",
        "messages": [
            {"role": "system", "content": "You are a helpful music genre analyst."},
            {"role": "user", "content": prompt}
        ]
    }

    try:
        # **Crucial Change: Use OpenRouter's API endpoint**
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions", # Changed endpoint!
            headers=headers,
            json=json_data
        )
        response.raise_for_status() # This will raise an HTTPError for 4xx/5xx responses
        return response.json()['choices'][0]['message']['content'].strip()
    except requests.exceptions.RequestException as e: # Catch specific request exceptions
        # More descriptive error handling for API issues
        if response is not None:
            return f"Failed to generate genre summary (HTTP {response.status_code}): {response.text} - {str(e)}"
        else:
            return f"Failed to generate genre summary: {str(e)}"
    except Exception as e:
        # Catch any other unexpected errors
        return f"An unexpected error occurred: {str(e)}"

############
# Isiah
############

def top_free_songs(df: pyspark.sql.DataFrame, state: str) -> pd.core.frame.DataFrame:
    """
    Filters df to free users and counts free user's top songs
    
    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe)
        free_status: If the user is a free subscriber

    Returns:
        filtered and aggregated dataframe 

    """

    # Filter for free users
    free_df = df.filter(col('subscription') == 'free')

    # Group by song, count the occurrences, and sort in descending order


    # Limit to top 5 songs and collect results
    if state == 'Nationwide':
        top_songs = free_df.groupBy('song').agg(count('*').alias('listens')).orderBy(col('listens').desc()).limit(5)
    else:
         top_songs = free_df.groupBy('state','song').agg(count('*').alias('listens'))\
            .orderBy(col('listens').desc()).filter(col('state')== state).limit(5)

    top_songs_pd = top_songs.toPandas().sort_values(by='listens', ascending=True)
    
    return top_songs_pd


def top_paid_songs(df: pyspark.sql.DataFrame, state: str) -> pd.core.frame.DataFrame:
    """
    Filters df to paid users and counts paid user's top songs

    Args:
        df (pyspark.sql.dataframe.Dataframe): dataframe
        paid_status: If the user is a paid subscriber

    Returns:
        filtered and aggregated dataframe 

    """

    # Filter for paid users
    paid_df = df.filter(col('subscription') == 'paid')

    # Group by song, count the occurrences, and sort in descending order
    

     # Limit to top 5 songs and collect results
    if state == 'Nationwide':
         top_songs = paid_df.groupBy('song').agg(count('*').alias('listens')).orderBy(col('listens').desc()).limit(5)
    else:
        top_songs = paid_df.groupBy('state','song').agg(count('*').alias('listens')) \
            .orderBy(col('listens').desc()).filter(col('state')== state).limit(5)

    top_songs_pd = top_songs.toPandas().sort_values(by='listens', ascending=True)
    
    return top_songs_pd


