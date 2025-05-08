import streamlit as st
import numpy as np
import plotly.express as px
import engine
import plotly.graph_objects as go
import altair as alt


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MuseDash PySpark") \
        .getOrCreate()

## Verify that SparkSession is created

try:
    df_listen = spark.read.json ('/Users/kunle/Python Projects/Kunles_Muse/Data/listen_events')
    print('Data loaded successfully')
except Exception as e:
    print(f'Error loading data: {e}')

# formatting transforming
cleaned_listen = engine.clean(df=df_listen)
artist_list = engine.get_artist_over(df=df_listen,number_of_lis=1000)

# makes page wide
st.set_page_config(layout = 'wide')
                   
# Streamlit Titling
st.title("Muse Dash")


#creating tabs
tab1, tab2 = st.tabs(["Map", "2nd for giggles"])

with tab1:
     
     with st.container():

        col_table = st.columns((5, 1, 5), gap='medium')
        # Sidebar
        st.sidebar.header("Select a State")
        available_states = cleaned_listen.select("state").distinct().orderBy("state").rdd.flatMap(lambda x: x).collect()
        selected_state = st.sidebar.selectbox("Filter by State (Optional):", 
                                            ['Nationwide'] + available_states,
                                            )
        
        if selected_state == 'Nationwide':
            top_10_header = "Top 10 National Artists"
            pie_title = "National Subscription Type Distribution"
        else:
            top_10_header = f"Top 10 Artists in {selected_state}"
            pie_title = f"Subscription Type Distribution in {selected_state}"


        with col_table[0]:
            # printing top ten chart
            top_10 = engine.get_top_10_artists(df=cleaned_listen, state=selected_state)
            st.header(top_10_header)
            st.dataframe(top_10, hide_index=True)

        
        with col_table[0]:
            # printing pie
            pie_df = engine.create_subscription_pie_chart(df=cleaned_listen, state=selected_state)


            chart = alt.Chart(pie_df).mark_arc().encode(
            theta=alt.Theta(field="count", type="quantitative"),
            color=alt.Color(field="subscription", type="nominal",
                            scale=alt.Scale(domain=['free', 'paid'],
                                            range=['red', 'green']),
                            legend=alt.Legend(title="Subscription Type", orient="bottom")),
            order=alt.Order(field="count", sort="descending"),
            tooltip=["subscription", "count"]
        ).properties(
            title=pie_title
        ).configure_view(
            fillOpacity=0  # Make the chart background transparent
        )
            st.altair_chart(chart)
            

        with col_table[2]:
            # listen chart creation
            listen_duration = engine.get_user_list(df=cleaned_listen, state=selected_state)

            # Determine the title based on the selected state
            if selected_state == "Nationwide":
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

        # st.sidebar.header("Select an Artist")
        option = st.selectbox(
        'Filter by Artist',
        artist_list,
        index=None,
        placeholder="Chosen Artist",
        accept_new_options = True
    )


    if option == None:
        st.write("You selected: ", option)
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
            geo_scope='usa', # limit map scope to USA
        )

        st.plotly_chart(fig)