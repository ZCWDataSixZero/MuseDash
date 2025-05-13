import streamlit as st
import numpy as np
import plotly.express as px
import kunle_engine
import plotly.graph_objects as go

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


# Initialize session state
if "option" not in st.session_state:
    st.session_state.option = "Kings Of Leon"

if "location" not in st.session_state:
    st.session_state.location = "Nationwide"

@st.fragment
def render_map(artist):
    b = kunle_engine.get_artist_state_listen(df=clean_listen, artist=artist)
    c = kunle_engine.map_prep_df(df=b)

    fig = go.Figure(data=go.Choropleth(
        locations=c.state,
        z=c.listens,
        locationmode='USA-states',
        colorscale='Blues',
        colorbar_title="Number of\n Listens"
    ))

    fig.update_layout(
        title_text=f'Number of {artist} Listens (2024-2025)',
        geo_scope='usa',
        margin={"r": 0, "t": 0, "l": 0, "b": 0}
    )

    event = st.plotly_chart(fig, on_select="rerun", selection_mode=["points", "box", "lasso"])
    points = event["selection"].get("points", [])

    if points:
        selected_state = points[0]["location"]
        if selected_state != st.session_state.location:
            st.session_state.location = selected_state
            st.rerun()
    else:
        # No state selected — clicked on whitespace
        if st.session_state.location != "Nationwide":
            st.session_state.location = "Nationwide"
            st.rerun()

# Layout
st.set_page_config(layout='wide')
col_table = st.columns((5, 10), gap='medium')

with col_table[0]:
    # Filter artist list by selected state
    top_10 = kunle_engine.get_top_10_artists(df=clean_listen, state=st.session_state.location)
    
    state_text = st.session_state.location if st.session_state.location != 'Nationwide' else "the Nation"
    st.header(f"Top 10 Artists in {state_text}")
    
    selected_row = st.dataframe(top_10,
                                use_container_width=True,
                                selection_mode='single-row',
                                on_select='rerun',
                                hide_index=True)

    rows = selected_row['selection'].get('rows', [])
    if rows:
        row_item = rows[0]
        selected_artist = top_10.artist[row_item]
        if selected_artist != st.session_state.option:
            st.session_state.option = selected_artist
            st.rerun()

with col_table[1]:
    st.subheader(f"You selected: {st.session_state.option}")
    render_map(st.session_state.option)