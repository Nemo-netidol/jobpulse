from RAG import LLMService, SimpleRetrievalStrategy
from services.vector_db.QdrantService import QdrantService
from services.BigQueryService import BigQueryService
import streamlit as st
from streamlit_echarts import st_echarts
import os

# Load connection settings from environment
QDRANT_API_KEY = os.getenv('QDRANT_API_KEY')
QDRANT_ENDPOINT = os.getenv('QDRANT_CLUSTER_ENDPOINT', 'http://qdrant:6333')
QDRANT_LOCAL_MODE = os.getenv('QDRANT_LOCAL_MODE', 'True').lower() == 'true'

# Initialize Services
@st.cache_resource
def init_services():
    qdrant = QdrantService(
        API_KEY=QDRANT_API_KEY, 
        url=QDRANT_ENDPOINT, 
        local=QDRANT_LOCAL_MODE
    )
    strategy = SimpleRetrievalStrategy()
    llm = LLMService(qdrant, strategy)
    
    # Mock Hook for BigQuery (since we are outside Airflow)
    class BQHook:
        def get_client(self):
            from google.cloud import bigquery
            # Assumes gcp-key.json is in root
            return bigquery.Client.from_service_account_json("gcp-key.json")
    
    bq_service = BigQueryService(BQHook())
    return qdrant, llm, bq_service

qdrant_service, llm_service, bq_service = init_services()

# --- Cached Data Fetching ---
@st.cache_data(ttl=3600)
def fetch_analytics():
    return bq_service.get_category_stats()

@st.cache_data(ttl=600)
def get_total_count():
    stats = qdrant_service.get_stats()
    return stats.get("total_points", 0)

data_count = get_total_count()

# --- UI Layout ---
st.set_page_config(page_title="JobPulse", page_icon="🚀", layout="wide")

st.title("JobPulse", anchor=False)

# Create Tabs for Authority
tab_chat, tab_insights = st.tabs(["💬 Job Assistant", "📊 Market Insights"])

with tab_chat:
    st.caption(f"🚀 Connected to Qdrant Cloud | {data_count} jobs indexed")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    has_message_history = len(st.session_state.messages) > 0

    SUGGESTIONS = {
        "Find me AI Engineer jobs": ":material/smart_toy: AI Engineer",
        "Find me Data Engineer jobs": ":material/storage: Data Engineer",
        "Find me Software Engineer jobs": ":material/code: Software Engineer",
        "Find me Machine Learning Engineer jobs": ":material/model_training: ML Engineer",
    }

    if not has_message_history:
        selection = st.pills("Suggested Searches", list(SUGGESTIONS.keys()), format_func=lambda x: SUGGESTIONS[x] )
        if selection:
            st.session_state.messages.append({"role": "user", "content": selection})
            with st.spinner("Searching..."):
                response  = llm_service.query(selection)
            st.session_state.messages.append({"role": "ai", "content": response})
            st.rerun()

    # 1. Display Chat History FIRST
    for message in st.session_state.messages:
        with st.chat_message(message['role']):
            st.markdown(message["content"])

    # 2. Display Chat Input LAST
    if prompt := st.chat_input("Ask about jobs..."):
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.spinner("Searching Qdrant..."):
            response  = llm_service.query(prompt)
        st.session_state.messages.append({"role": "ai", "content": response})
        st.rerun()

with tab_insights:
    st.subheader("Live Market Composition")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.metric("Total Jobs Indexed", data_count)
        cat_data = fetch_analytics()
        if cat_data:
            top_cat = cat_data[0]['name']
            st.info(f"🔥 **{top_cat}** is currently the most active category.")

    with col2:
        if cat_data:
            options = {
                "tooltip": {"trigger": "item"},
                "legend": {"top": "5%", "left": "center"},
                "series": [
                    {
                        "name": "Job Categories",
                        "type": "pie",
                        "radius": ["40%", "70%"],
                        "avoidLabelOverlap": False,
                        "itemStyle": {
                            "borderRadius": 10,
                            "borderColor": "#fff",
                            "borderWidth": 2,
                        },
                        "label": {"show": False, "position": "center"},
                        "emphasis": {
                            "label": {"show": True, "fontSize": "20", "fontWeight": "bold"}
                        },
                        "labelLine": {"show": False},
                        "data": cat_data,
                    }
                ],
            }
            st_echarts(options=options, height="500px")
        else:
            st.info("No category data available yet.")
