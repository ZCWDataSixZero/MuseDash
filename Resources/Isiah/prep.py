import pyspark
# from pyspark.sql.functions import SparkSession
import os
from pyspark.sql.functions import col, count, desc
import matplotlib.pyplot as plt


def top_paid_artists(df: pyspark.sql.DataFrame, paid_status: str) -> pyspark.sql.DataFrame:
    """
    Filters df to paid users and counts paid user's top artists 

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe
        paid_status: If the user is a paid subscriber or not

    Returns:
        filtered and aggregated dataframe 

    """
    # Filter for paid users
    paid_df = df.filter(col('subscription') == 'paid')

    # Group by artist, count occurrences, and sort in descending order
    artist_counts = paid_df.groupBy('artist').agg(count('*').alias('count')).orderBy(col('count').desc())

    # Limit to top 5 artists and collect results
    top_artists = artist_counts.limit(5)


    return top_artists


def top_free_artists(df: pyspark.sql.DataFrame, free_status: str) -> pyspark.sql.DataFrame:
    """
    Filters df to free users and counts free user's top artists
    
    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe
        free_status: If the user is a free subscriber

    Returns:
        filtered and aggregated dataframe 

    """

    # Filter for free users
    free_df = df.filter(col('subscription') == 'free')

    # Group by artist, count occurrences, and sort in descending order
    free_artist_counts = free_df.groupBy('artist').agg(count('*').alias('count')).orderBy(col('count').desc())

    # Limit to top 5 artists and collect results
    top_artists = free_artist_counts.limit(5)

    return top_artists


def top_free_songs(df: pyspark.sql.DataFrame, free_status: str) -> pyspark.sql.DataFrame:
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
    song_counts = free_df.groupBy('song').agg(count('*').alias('count')).orderBy(col('count').desc)

    # Limit to top 5 songs and collect results
    top_songs = song_counts.limit(5).collect()

    return top_songs


def top_paid_songs(df: pyspark.sql.DataFrame, paid_status: str) -> pyspark.sql.DataFrame:
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
    song_counts = paid_df.groupBy('song').agg(count('*').alias('count')).orderBy(col('count').desc())

    # Limit to top 5 songs and collect results
    top_songs = song_counts.limit(5).collect()

    return top_songs