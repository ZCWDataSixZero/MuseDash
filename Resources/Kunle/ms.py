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

@st.cache_data
def kpis(_df):
    return e.calculate_kpis(df=_df)

@st.cache_data
def user_list(_df, state):
    return e.get_user_list(df = _df, state=state)

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
    # setting dashboard column layout
    col_table = st.columns((5, 10), gap='medium')    

    with st.container(border=True):

        with col_table[0]:
            with st.container(border=True):
                # creating top 10 chart dataframe
                top_10 = get_top_artists_by_state(clean_listen, st.session_state.location)
                state_text = st.session_state.location if st.session_state.location != "Nationwide" else "the Nation"
                st.header(f"Top 10 Artists in {state_text}")
                
                # creates the box to pic artist
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
            
        
            with st.container():
                # KPI metrics
                kpi_data = kpis(_df=clean_listen)
                col1, col2, col3 = st.columns([1.5, 2, 2.2])
                with col1:
                    with st.container(border=True):
                        st.metric("Total Users", f'{round(kpi_data[0]/1000)}k+')
                with col2:
                    with st.container(border=True):
                        st.metric("Average Total Listening", f"{round(kpi_data[1]/60)} MIN")
                with col3:
                    with st.container(border=True):
                        st.metric("Total Paid Listening", f"{round(kpi_data[2]/3600000)} HR")
            
            with st.container(border=True):
                # listen graph creation
                listen_duration = user_list(_df=clean_listen, state=st.session_state.location)
                chart_state = st.session_state.location if st.session_state.location != "Nationwide" else "the Nation"
                    
                #create the line graph
                line_fig = px.line(
                    listen_duration,
                    x="month_name",
                    y="total_duration",
                    color="subscription",
                    title= f'How long are users listening in {chart_state}',
                    labels={"month_name": "Month", "total_duration": "Total Duration (seconds)"}
                        )
                line_fig.update_layout(
                    hovermode="x unified",

                    #style the hover line color
                    xaxis=dict(
                        showspikes=True,
                        spikemode='across',
                        spikesnap='cursor',
                        spikethickness=1,
                        spikecolor="lightgray"
                    ),
                    yaxis=dict(
                        showspikes=False, #turns off horizontal line

                    )
                                        )

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
                
        with col_table[1]:
            with st.container(border=True):
                st.subheader(f"Number of {st.session_state.option} Listens")
                render_map(st.session_state.option)



### ------------------ MAIN UI: TAB 23 ------------------
with tab3:
    # Streamlit Titling
    #st.markdown("<h1 style='text-align: center;'><span style='color: white'>Muse</span><span style='color: blue;'>Dash</span></h1>", unsafe_allow_html=True)
    pass

