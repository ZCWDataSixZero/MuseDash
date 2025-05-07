import pandas as pd
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, year, month, date_format, sum, when

# Initialize SparkSession
@st.cache_resource
def create_spark_session():
    return SparkSession.builder.appName("Streamlit PySpark").getOrCreate()

def main():
    #pass
    # set_page_config()
    listening_duration = load_data()
    if listening_duration is not None:
        update_subscription_status_and_visualize(listening_duration)
    else: st.error("Failed to load data.")

    

def set_page_config():
    st.set_page_config(
        page_title = "Listening Duration Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown("<style> footer {visibility: hideen;} </style>", unsafe_allow_html=True )


def load_data():
    file_path = "listen_events"
    spark = create_spark_session()

    try:
        listening_duration = spark.read.json(file_path)

        # st.write("### DataFrame Preview:")
        # st.dataframe(listening_duration)
        return listening_duration
    except Exception as e:
        st.error(f"Error loading file: {e}")
    return None

def update_subscription_status_and_visualize(listening_duration):
    '''
    Updates subscription status in listening_duration from 'free' to 'paid' for users who are
    already paid subscribers and then visualizes the data
    '''
    try: 
        # Find the paid users
        paid_users = (
            listening_duration.filter(col("level") == "paid")
            .select("userId")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        #Update subscription of free to paid users from 'free' to 'paid'
        updated_listening_duration = listening_duration.withColumn(
            "level",
            when(col("userId").isin(paid_users), "paid").otherwise(col("level"))
        )

        #Convert 'ts' column to readable date-time
        updated_listening_duration = updated_listening_duration.withColumn(
            "ts", to_timestamp(col("ts").cast("long") / 1000)
        )

        # Extract year, month, and month name from the timestamp column
        updated_listening_duration = updated_listening_duration.withColumn("year", year(col("ts"))) \
            .withColumn("month", month(col("ts"))) \
            .withColumn("month_name", date_format(col("ts"), "MMMM"))
        
        # Creating dropdown to filter by state in alphabetical order
        states = updated_listening_duration.select("state").distinct().rdd.flatMap(lambda x: x).collect()
        states.sort()
        selected_states = st.multiselect("Select State(s):", states, default=[])

        #Update the tile based on selected states
        if selected_states:
            st.write(f"### How long are users listening in {', '.join(selected_states)}?")
        else:
            st.write("### How long are users listening in the USA?")

        # Filter data on selected states
        if selected_states:
         updated_listening_duration = updated_listening_duration.filter(col("state").isin(selected_states))
        
        # Group by year, month, subscription, and month_name, then sum the durations
        duration_grouped = updated_listening_duration.groupBy("year", "month", "month_name", "level") \
            .agg(sum("duration").alias("total_duration")) \
            .orderBy("year", "month", "level")

        #convert to a pandas dataframe
        updated_listening_duration_pd = duration_grouped.toPandas()

        #create the line chart

        line_fig = px.line(
            updated_listening_duration_pd,
            x="month_name",
            y="total_duration",
            color="level",
            # title="Paid users listen to 7 years worth of listening hours more than free",
            labels={"month_name": "Month", "total_duration": "Total Duration (seconds)"}
        )
        st.plotly_chart(line_fig)



    except Exception as e:
        st.error(f"An error occured while updating: {e}")

if __name__ == '__main__':
    main()

