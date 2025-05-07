import streamlit as st
import numpy as np
import plotly.express as px
import MusicDB
import plotly.graph_objects as go

# fig = px.choropleth(locations=["CA", "TX", "NY",'AK'], locationmode="USA-states", color=[1,2,3,4], scope="usa")


# st.plotly_chart(fig)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Museh PySpark Learning") \
        .getOrCreate()

## Verify that SparkSession is created

try:
    df_listen = spark.read.json ('./Data/listen_events')
    print('Data loaded successfully')
except Exception as e:
    print(f'Error loading data: {e}')



# list of artists with a certain number
artist_list = MusicDB.get_arist_over_1000(df=df_listen,number_of_lis=1000)


#selected_artist = st.sidebar.selectbox()




#creating tabs
tab1, tab2 = st.tabs(["Map", "2nd for giggles"])

with tab1:
    with st.container():
        option = st.selectbox(
        'Select an Artist',
        artist_list,
        index=None,
        placeholder="Choosen Artis",
        accept_new_options = True
    )
        st.write("You selected: ", option)



    if option == None:
        pass
    else:
        # creating the dataframe of listens for specific artists
        b = MusicDB.get_artist_state_listen(df=df_listen, artist=option)

        # filtering data to what is needed to make map
        c = MusicDB.map_prep_df(df=b)
        ## creating the maps
        fig = go.Figure(data=go.Choropleth(
            locations=c.state, # Spatial coordinates
            z = c.listens, # Data to be color-coded
            locationmode = 'USA-states', # set of locations match entries in `locations`
            colorscale = 'Blues',
            colorbar_title = "Number of Listens",
        ))

        # adding context to the map
        fig.update_layout(
            title_text = f'Number of {option} Listens \n 2024-2025',
            geo_scope='usa', # limite map scope to USA
        )

        st.plotly_chart(fig)



with tab2:
    option = None
    if st.button("Send balloons!"):
        st.balloons()