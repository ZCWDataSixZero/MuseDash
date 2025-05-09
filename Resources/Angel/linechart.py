import streamlit as st
import numpy as np
import plotly.express as px
import angelmethod
import plotly.graph_objects as go

# fig = px.choropleth(locations=["CA", "TX", "NY",'AK'], locationmode="USA-states", color=[1,2,3,4], scope="usa")


# st.plotly_chart(fig)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Museh PySpark Learning") \
        .getOrCreate()

## Verify that SparkSession is created

try:
    df_listen = spark.read.json ('/Users/angel/Downloads/spring25data/listen_events')
    print('Data loaded successfully')
except Exception as e:
    print(f'Error loading data: {e}')


a = angelmethod.clean(df=df_listen)



# Sidebar
st.sidebar.header("Select a State")
available_states = a.select("state").distinct().orderBy("state").rdd.flatMap(lambda x: x).collect()
selected_state = st.sidebar.selectbox("Filter by State (Optional):", ["All"] + available_states)

b = angelmethod.get_user_list(df=a, selected_states=selected_state)


#Mock KPIs until I can get the real thing working
col1, col2, col3 = st.columns(3)
col1.metric("Total Users", "1k+", "500")
col2.metric("Average Listening Hours", "9 hours", "-2%")
col3.metric("Average Songs per Session", "7", "3")



# Determine the title based on the selected state
if selected_state == "All":
    chart_title = "How long are users listening in the USA?"
else:
    chart_title = f"How long are users listening in {selected_state}?"

#create the line chart
line_fig = px.line(
    b,
    x="month_name",
    y="total_duration",
    color="subscription",
    title=chart_title,
    labels={"month_name": "Month", "total_duration": "Total Duration (seconds)"}
        )


line_fig.update_traces(
    hovertemplate = f'<span style="font-size: 14px;">' +
                    "<b>%{x}</b><br>" +
                    "%{y} seconds" +
                    '</span><extra></extra>'
    
)
st.plotly_chart(line_fig)