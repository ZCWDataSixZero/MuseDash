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
    df_listen = spark.read.json ('/Users/kunle/Python Projects/Kunles_Muse/Data/listen_events')
    print('Data loaded successfully')
except Exception as e:
    print(f'Error loading data: {e}')


cleaned_listen = engine.clean(df=df_listen)


# Sidebar
st.sidebar.header("Select a State")
available_states = cleaned_listen.select("state").distinct().orderBy("state").rdd.flatMap(lambda x: x).collect()
selected_state = st.sidebar.selectbox("Filter by State (Optional):", 
                                      ['All'] + available_states,
                                      
                                      )

# printing top ten chart
top_10 = engine.get_top_10_artists(df=cleaned_listen, state=selected_state)
st.table(top_10)




listen_duration = engine.get_user_list(df=cleaned_listen, state=selected_state)

# Determine the title based on the selected state
if selected_state == "All":
    chart_title = "How long are users listening in the USA?"
else:
    chart_title = f"How long are users listening in {selected_state}?"
    
#create the line chart
line_fig = px.line(
    listen_duration,
    x="month_name",
    y="total_duration",
    color="subscription",
    title=chart_title,
    labels={"month_name": "Month", "total_duration": "Total Duration (seconds)"}
        )
st.plotly_chart(line_fig)