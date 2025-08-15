# 🎵 MuseDash — Music Streaming Analytics Dashboard  

## 📌 Overview  
MuseDash is an interactive music analytics dashboard built to analyze **historical streaming data** from Zip-pot-ify, a fictional but nationwide music platform. The project showcases our ability to design and deploy a **full data engineering and analytics pipeline** — from raw data ingestion to dynamic visualizations — highlighting **regional listening trends, artist popularity, genre breakdowns, and time-based metrics**.  

---

## 🚀 Tech Stack  

| Layer | Tools & Technologies |
|-------|----------------------|
| **Data Ingestion & Storage** | **AWS S3**, **AWS IAM** |
| **Data Processing** | **PySpark**, **Pandas** |
| **Visualization** | **Altair**, **Plotly** |
| **Application Layer** | **Streamlit** |
| **Version Control & Collaboration** | **GitHub** |

---

## 🔄 Pipeline Code Highlights  

### 1️⃣ Data Ingestion  
```python
import pandas as pd
import pyspark
from pyspark.sql.functions import desc, asc, avg, count, col, when, to_timestamp, year, month, date_format, sum, when, udf, from_unixtime
from pyspark.sql.types import StringType
import requests
import json
from apikey import API_KEY
from transformers import T5Tokenizer, T5ForConditionalGeneration
import torch

############
# Kunle
############

def get_artist_state_listen( df: pyspark.sql.dataframe.DataFrame , artist: str) -> pyspark.sql.dataframe.DataFrame:
    '''
    Filters and aggregates a pyspark dataframe to count listens by artist and state

    Args:
        df (pyspark.sql.dataframe.DataFrame): dataframe
```

### 2️⃣ PySpark Data Processing  
```python
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
```

### 3️⃣ Streamlit Visualization  
```python
st.cache_resource
st.cache_resource
st.cache_resource
st.cache_data
st.cache_data
st.cache_resource
st.cache_data
st.cache_data
st.cache_data
st.cache_data
```

---

## 📊 Dashboard Features  
- Choropleth Maps — visualize listening habits across U.S. states.  
- Artist & Genre Filters — deep dive into specific music categories.  
- Time-based Trends — track popularity shifts over time.  

---

## 💡 Portfolio Value  
This project demonstrates:  
- End-to-end pipeline engineering  
- Scalable big data processing with PySpark  
- Interactive dashboard deployment with Streamlit  
