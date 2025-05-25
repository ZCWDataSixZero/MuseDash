import pandas as pd
import streamlit as st
import numpy as np
import plotly.express as px
import angelmethod 
from angelmethod import build_prompt_from_dataframe, load_flan_model
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from transformers import pipeline, T5Tokenizer, T5ForConditionalGeneration
import torch



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

# Define order of months
month_order_list = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

#dict comprehension to create a dictionary for month mapping
month_mapping = {month: i + 1 for i, month in enumerate(month_order_list)}

# Create a new column 'month_number' giving each month a number in the dataframe
b['month_number'] = b['month_name'].map(month_mapping)

#create the slider
month_slider = st.slider(
  
    label="Select a range of months",
    min_value=1,
    max_value=12,
    value=(1, 12), #slider starts at May and ends at December
    format="%i",  #display as integer
    label_visibility="visible",
    help="Add or remove months to filter listening data",
)

#grab selected month numbers
start_month, end_month = month_slider

#Filter the DataFrame based on the selected month range
filtered_b = b[(b['month_number'] >= start_month) & (b['month_number'] <= end_month)]

st.write("Preview of filtered data going into summary:")
st.dataframe(filtered_b.head())

#create the line chart with filtered dataframe
line_fig = px.line(
    filtered_b,
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
# if not filtered_b.empty:
#     summary_input_text = ""
#     for index, row in filtered_b.iterrows():
#         summary_input_text += f"{row['month_name']}: Total duration was {row['total_duration']:.2f} seconds for {row['subscription']} users. "
#     if summary_input_text:
#         st.subheader("AI Summarization of Listening Data")
#         try:
#             summary_output = summarizer(summary_input_text, max_length=40, min_length=23, do_sample=False)[0]['summary_text']
#             st.write(summary_output)
#         except Exception as e:
#             st.write(f"Error generating summary: {e}")
#     else:
#         st.write("No data available for the selected months.")
# else:
#     st.info("Please select a valid month range.")
st.plotly_chart(line_fig)

# Summarization

# text_summary = get_summaries(df=a)

# with st.spinner("Summarizing..."):
#     try:
#         summarizer = pipeline("summarization", model="t5-small", device=-1)
#         print("Model loaded successfully!")
#         summary = summarizer(text_summary, max_length=30, min_length=25, do_sample=False)[0]['summary_text']
#         st.write(summary)
#         st.subheader("Summarized by AI")
#     except Exception as e:
#         st.write(f"Error generating summary: {e}")
#         st.error("Failed to load the summarization model. Please try again later.")




try:
# Load model after Spark and data prep
    tokenizer, model = load_flan_model()

    # Generate summary prompt
    if not filtered_b.empty:
        prompt_text = build_prompt_from_dataframe(filtered_b)
        with st.spinner("Generating summary..."):   
            # Tokenize and generate summary
            inputs = tokenizer(prompt_text, return_tensors="pt", truncation=True, max_length=512)
            outputs = model.generate(**inputs, max_length=200, min_length=150, do_sample=True, top_p=0.95, top_k=50, early_stopping=True)
            summary = tokenizer.decode(outputs[0], skip_special_tokens=True)

            
            st.write(summary)
            st.markdown("<p style='font-size: 0.85em; color: gray;'>AI-generated summary</p>", unsafe_allow_html=True)

    else:
            st.info("No data available to summarize for selected filters.")
except Exception as e:
    st.error(f"Error loading model or generating summary: {e}")
    st.write("Please check your model and data.")