import streamlit as st
import numpy as np
import plotly.express as px
import kunle_engine
import plotly.graph_objects as go

# fig = px.choropleth(locations=["CA", "TX", "NY",'AK'], locationmode="USA-states", color=[1,2,3,4], scope="usa")


# st.plotly_chart(fig)

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


clean_listen = kunle_engine.clean(df=df_listen)
# list of artists with a certain number
artist_list = kunle_engine.get_artist_over_1000(df=clean_listen,number_of_lis=1000)


#selected_artist = st.sidebar.selectbox()




#creating tabs
tab1, tab2 = st.tabs(["Map", "2nd for giggles"])

with tab1:
    with st.container():


        option = st.selectbox(
        'Select an Artist',
        artist_list,
        index=None,
        placeholder="Chosen Artist",
        accept_new_options = True
    )
        st.write("You selected: ", option)



    if option == None:
        pass
    else:
        # creating the dataframe of listens for specific artists
        b = kunle_engine.get_artist_state_listen(df=clean_listen, artist=option)

        # filtering data to what is needed to make map
        c = kunle_engine.map_prep_df(df=b)
        ## creating the maps
        fig = go.Figure(data=go.Choropleth(
            locations=c.state, # Spatial coordinates
            z = c.listens, # Data to be color-coded
            locationmode = 'USA-states', # set of locations match entries in `locations`
            colorscale = 'Blues',
            colorbar_title = "Number of\n Listens"
        ))

        # adding context to the map
        fig.update_layout(
            title_text = f'Number of {option} Listens \n 2024-2025',
            geo_scope='usa', # limit map scope to USA
        )

        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

        event = st.plotly_chart(fig, on_select="rerun", selection_mode=["points","box","lasso"])

        points = event["selection"].get("points", [])
        
        #sigla = first_point["properties"].get("sigla", None)
        #st.plotly_chart(fig)  points = event["selection"].get("points", [])
        if points:
            first_point = points[0]
            location = first_point['location']
            st.write("You selected: ", location)
        else:
            st.write("You selected: ", 'Nationwide')

        #event = st.plotly_chart(fig, on_select="rerun", selection_mode=["points","box","lasso"])

        #st.subheader("You selected: ", event)

        # st.dataframe(b.toPandas(), hide_index=True)



with tab2:
    option = None
    if st.button("Send balloons!"):
        st.balloons()