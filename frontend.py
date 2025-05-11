import streamlit as st
import numpy as np
import plotly.express as px
import engine
import plotly.graph_objects as go
import altair as alt


from pyspark.sql import SparkSession

# makes page wide
st.set_page_config(layout = 'wide')

spark = SparkSession.builder \
    .appName("MuseDash PySpark") \
        .getOrCreate()

## Verify that SparkSession is created

try:
    df_listen = spark.read.json ('/Users/jim/Projects/p1/spring25data/Data/listen_events')
    print('Data loaded successfully')
except Exception as e:
    print(f'Error loading data: {e}')

# formatting transforming
cleaned_listen = engine.clean(df=df_listen)
artist_list = engine.get_artist_over(df=cleaned_listen,number_of_lis=1000)

# allow .css formatting
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

local_css("style.css")
                   
# Streamlit Titling
st.title("Muse Dash")

col_table = st.columns((5, 10), gap='medium')

# Sidebar
st.sidebar.header("Select a State")
available_states = engine.get_states_list(cleaned_listen)
selected_state = st.sidebar.selectbox("Filter by State (Optional):", 
                                    ['Nationwide'] + available_states,
                                    )
# titles depending on state selected
if selected_state == 'Nationwide':
    top_10_header = "Top 10 National Artists"
    pie_title = "National Subscription Type Distribution"
    paid_title = 'Top Songs for Paid Users'
    free_title = 'Top Songs for Free Users'
else:
    top_10_header = f"Top 10 Artists in {selected_state}"
    pie_title = f"Subscription Type Distribution in {selected_state}"
    paid_title = f'Top Songs for Paid Users in {selected_state}'
    free_title = f'Top Songs for Free Users in {selected_state}'


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
                                    range=['orange', 'blue']),
                    legend=alt.Legend(title="Subscription Type", orient="bottom")),
    order=alt.Order(field="count", sort="descending"),
    tooltip=["subscription", "count"]
).properties(
    title=pie_title
).configure_view(
    fillOpacity=0  # Make the chart background transparent
)
    st.altair_chart(chart)
    
with col_table[1]:
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
        b = engine.get_artist_state_listen(df=cleaned_listen, artist=option)

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

with col_table[1]:
    col_free, col_paid, col_line = st.columns(3)
    with col_paid:
        # paid songs charts
        st.subheader(paid_title)
        paid_songs_df = engine.top_paid_songs(df=cleaned_listen, state=selected_state)

        chart_paid_songs = alt.Chart(paid_songs_df).mark_bar().encode(
            x=alt.X('listens:Q', title='Listens'),
            y=alt.Y('song:N', sort='-x', title=None),
            tooltip=['song', 'listens']
        ).properties(
            width=700,
            height=400,
        ).configure_axis(
            labelFontSize=14 
        )
        st.altair_chart(chart_paid_songs, use_container_width=True)            

    with col_free:
    # free songs chart
        st.subheader(free_title)
        free_songs_df = engine.top_free_songs(df=cleaned_listen, state=selected_state)
        
        chart_free_songs = alt.Chart(free_songs_df).mark_bar().encode(
            x=alt.X('listens:Q', title='Listens'),
            y=alt.Y('song:N', sort='-x', title=None),
            tooltip=['song', 'listens']
        ).properties(
            width=700,
            height=400,
        ).configure_axis(
            labelFontSize=14 
        )
        st.altair_chart(chart_free_songs, use_container_width=True)
    
        with col_line:
            # listen graph creation
            listen_duration = engine.get_user_list(df=cleaned_listen, state=selected_state)

            # Determine the title based on the selected state
            if selected_state == "Nationwide":
                chart_title = "How long are users listening in the USA?"
            else:
                chart_title = f"How long are users listening in {selected_state}?"
                
            #create the line graph
            line_fig = px.line(
                listen_duration,
                x="month_name",
                y="total_duration",
                color="subscription",
                title=chart_title,
                labels={"month_name": "Month", "total_duration": "Total Duration (seconds)"}
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
