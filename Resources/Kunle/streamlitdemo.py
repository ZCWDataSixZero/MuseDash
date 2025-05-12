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

# makes page wide
st.set_page_config(layout = 'wide')
#selected_artist = st.sidebar.selectbox()
col_table = st.columns((5, 10), gap='medium')                                  



location = 'Nationwide'
#option = None
#creating tabs
tab1, tab2 = st.tabs(["Map", "2nd for giggles"])

map_container = st.container(border=True)
df_container = st.container(border=True)

with tab1:

    with col_table[1]:
        with map_container:
            # option = st.selectbox(
            # 'Select an Artist',
            # artist_list,
            # #index=None,
            # placeholder="Chosen Artist",
            # accept_new_options = True
            # )

            if 'option' not in st.session_state:
                pass
            else:

                st.write("You selected: ", st.session_state.option)



                # if not option:
                #     pass
                # else:
                # creating the dataframe of listens for specific artists
                b = kunle_engine.get_artist_state_listen(df=clean_listen, artist=st.session_state.option)

                # filtering data to what is needed to make map
                c = kunle_engine.map_prep_df(df=b)
                c_max = c['listens'].max()
                c_min = c['listens'].min()
                ## creating the maps
                fig = go.Figure(data=go.Choropleth(
                    locations=c.state, # Spatial coordinates
                    z = c.listens, # Data to be color-coded
                    locationmode = 'USA-states', # set of locations match entries in `locations`
                    colorscale = 'Blues',
                    #range_color=(c_min, c_max),
                    colorbar_title = "Number of\n Listens"
                ))

                # adding context to the map
                fig.update_layout(
                    title_text = f'Number of {st.session_state.option} Listens \n 2024-2025',
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
            # if not location:
            #     selected_state = "Nationwide"
            # else:
            #     selected_state = location
    selected_state = location
    # titles depending on state selected
    if selected_state == 'Nationwide' :
        top_10_header = "Top 10 National Artists"
        pie_title = "National Subscription Type Distribution"
        paid_title = 'Top Songs for Paid Users'
        free_title = 'Top Songs for Free Users'
    else:
        top_10_header = f"Top 10 Artists in {location}"
        pie_title = f"Subscription Type Distribution in {location}"
        paid_title = f'Top Songs for Paid Users in {location}'
        free_title = f'Top Songs for Free Users in {location}'
    
    with col_table[0]:
        with df_container:
            top_10 = kunle_engine.get_top_10_artists(df=clean_listen, state=location)
            st.header(top_10_header)
            selected_row = st.dataframe(top_10,
                                        use_container_width=True,
                                        selection_mode='single-row',
                                        on_select='rerun',
                                        hide_index=True)
            
            row_item = selected_row['selection'].get('rows')[0]
            
            if row_item:
                st.session_state.option = top_10.artist[row_item]
                #st.rerun()



with tab2:
    option = None
    if st.button("Send balloons!"):
        st.balloons()