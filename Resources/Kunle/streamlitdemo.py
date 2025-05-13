import streamlit as st
import plotly.graph_objects as go
from pyspark.sql import SparkSession
import kunle_engine

### ------------------ CACHED SETUP ------------------

@st.cache_resource
def get_spark_session():
    return SparkSession.builder.appName("Museh PySpark Learning").getOrCreate()

@st.cache_resource
def load_data():
    return spark.read.json('/Users/kunle/Python Projects/Kunles_Muse/Data/listen_events')

@st.cache_resource
def get_clean_data():
    return kunle_engine.clean(df=load_data())

@st.cache_data
def get_artist_list(df, threshold=1000):
    return kunle_engine.get_artist_over_1000(df=df, number_of_lis=threshold)

@st.cache_data
def get_top_artists_by_state(_df, state):
    return kunle_engine.get_top_10_artists(df=_df, state=state)

@st.cache_resource
def get_map_data(_df, artist):
    artist_df = kunle_engine.get_artist_state_listen(df=_df, artist=artist)
    return kunle_engine.map_prep_df(df=artist_df)

### ------------------ INITIAL STATE ------------------

if "option" not in st.session_state:
    st.session_state.option = "Kings Of Leon"

if "location" not in st.session_state:
    st.session_state.location = "Nationwide"

### ------------------ PAGE CONFIG ------------------

st.set_page_config(layout="wide")
col_table = st.columns((5, 10), gap="medium")
tab1, tab2 = st.tabs(["Map", "2nd for giggles"])

spark = get_spark_session()
clean_listen = get_clean_data()

### ------------------ MAP RENDER FUNCTION ------------------

def render_map(artist):
    c = get_map_data(clean_listen, artist)

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
        # If background is clicked (no state), reset to Nationwide
        if st.session_state.location != "Nationwide":
            st.session_state.location = "Nationwide"
            st.rerun()

### ------------------ MAIN UI: TAB 1 ------------------

with tab1:
    with st.container():
        with col_table[0]:
            top_10 = get_top_artists_by_state(clean_listen, st.session_state.location)
            state_text = st.session_state.location if st.session_state.location != "Nationwide" else "the Nation"
            st.header(f"Top 10 Artists in {state_text}")
            
            selected_row = st.dataframe(
                top_10,
                use_container_width=True,
                selection_mode="single-row",
                on_select="rerun",
                hide_index=True
            )

            rows = selected_row['selection'].get("rows", [])
            if rows:
                selected_artist = top_10.artist[rows[0]]
                if selected_artist != st.session_state.option:
                    st.session_state.option = selected_artist
                    st.rerun()

        with col_table[1]:
            st.subheader(f"You selected: {st.session_state.option}")
            render_map(st.session_state.option)

### ------------------ FUN TAB ------------------

with tab2:
    with st.container():
        if st.button("Send balloons!"):
            st.balloons()
