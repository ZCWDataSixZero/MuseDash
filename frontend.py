import streamlit as st
import numpy as np
import plotly.express as px
import engine
import plotly.graph_objects as go
import altair as alt


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
artist_list = engine.get_arist_over_1000(df=df_listen,number_of_lis=1000)

#creating tabs
tab1, tab2 = st.tabs(["Map", "2nd for giggles"])

with tab1:
     with st.container():
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

with tab2:
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
        b = engine.get_artist_state_listen(df=df_listen, artist=option)

        # filtering data to what is needed to make map
        c = engine.map_prep_df(df=b)
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
            geo_scope='usa', # limite map scope to USA
        )

        st.plotly_chart(fig)