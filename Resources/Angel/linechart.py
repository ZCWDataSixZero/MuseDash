import streamlit as st
import numpy as np
import plotly.express as px
import angelmethod
import plotly.graph_objects as go
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Museh PySpark Learning") \
        .getOrCreate()

## Verify that SparkSession is created

try:
    df_listen = spark.read.json ('/Users/angel/Downloads/spring25data/app/listen_events')
    print('Data loaded successfully')
except Exception as e:
    print(f'Error loading data: {e}')

total_users, average_listening_time, total_duration_sum = angelmethod.calculate_kpis(df=df_listen)

print(total_users, average_listening_time, total_duration_sum)

a = angelmethod.clean(df=df_listen)



# Sidebar
st.sidebar.header("Select a State")
available_states = a.select("state").distinct().orderBy("state").rdd.flatMap(lambda x: x).collect()
selected_state = st.sidebar.selectbox("Filter by State (Optional):", ["All"] + available_states)

b = angelmethod.get_user_list(df=a, selected_states=selected_state)


#Create KPIs
col1, col2, col3 = st.columns(3)
st.metric("Total Users", "1k+")
st.metric("Average Listening in Minutes", f"{average_listening_time / 60:.2f} minutes")
st.metric("Total Listening Hours(Paid)", f"{total_duration_sum / 3600:.2f} hours")




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

#change color of the lines
line_fig.update_traces(
    selector={'name': 'paid'},
    line=dict(color='orange', width=4),
    name='Paid'
)

line_fig.update_layout(hovermode="x unified")

# Update hovertemplate for the 'Paid' trace
line_fig.update_traces(
    selector={'name': 'paid'},
    hovertemplate='<span style="font-size: 18px;">' +
                  'Paid: %{y:.2f}' +
                  '<extra></extra>'
)

# Update hovertemplate for the 'Free' trace
line_fig.update_traces(
    selector={'name': 'free'},
    hovertemplate='<span style="font-size: 18px;">' +
                  'Free: %{y:.2f}' +
                  '<extra></extra>',
  
)
st.plotly_chart(line_fig)