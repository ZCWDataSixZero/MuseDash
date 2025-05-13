import streamlit as st
import plotly.express as px
import e
import plotly.graph_objects as go
import altair as alt
from pyspark.sql import SparkSession

### ------------------ CACHED SETUP ------------------

@st.cache_resource
def get_spark_session():
    return SparkSession.builder.appName("Museh PySpark Learning").getOrCreate()

@st.cache_resource
def load_data():
    return spark.read.json('/Users/kunle/Python Projects/Kunles_Muse/Data/listen_events')

@st.cache_resource
def get_clean_data():
    return e.clean(df=load_data())

@st.cache_data
def get_artist_list(df, threshold=1000):
    return e.get_artist_over_1000(df=df, number_of_lis=threshold)

@st.cache_data
def get_top_artists_by_state(_df, state):
    return e.get_top_10_artists(df=_df, state=state)

@st.cache_resource
def get_map_data(_df, artist):
    artist_df = e.get_artist_state_listen(df=_df, artist=artist)
    return e.map_prep_df(df=artist_df)

### ------------------ INITIAL STATE ------------------

if "option" not in st.session_state:
    st.session_state.option = "Kings Of Leon"

if "location" not in st.session_state:
    st.session_state.location = "Nationwide"

### ------------------ PAGE CONFIG ------------------
st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align: center;'><span style='color: white'>Muse</span><span style='color: blue;'>Dash</span></h1>", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Pipeline", "Dashboard", "Repo"])

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
    # Streamlit Titling
    pass

### ------------------ MAIN UI: TAB 2 ------------------
with tab2:
    # Streamlit Titling
    #st.markdown("<h1 style='text-align: center;'><span style='color: white'>Muse</span><span style='color: blue;'>Dash</span></h1>", unsafe_allow_html=True)
    pass

### ------------------ MAIN UI: TAB 23 ------------------
with tab3:
    # Streamlit Titling
    #st.markdown("<h1 style='text-align: center;'><span style='color: white'>Muse</span><span style='color: blue;'>Dash</span></h1>", unsafe_allow_html=True)
    pass

