from RAG.LLM_service import LLMService
from dags.include.vector_db import VectorDatabase
import streamlit as st
import time
import os
from scripts.seed_db import seed_databases

CHROMA_DIR = "data/chroma_db"

vector_db = VectorDatabase(CHROMA_DIR)
llm_service = LLMService(vector_db)
# Helper to get current counts
def get_db_stats():
    try:
        data_count = vector_db.get_data_count()
        return data_count
    except Exception as e:
        st.sidebar.error(f"Error getting Chroma count: {e}")
        return 0

data_count = get_db_stats()

def check_seed():
    if "HF_TOKEN" not in os.environ: 
        st.error("HF_TOKEN missing! Please add it to Streamlit Secrets or .env file.")
        st.info("Go to Settings -> Secrets and add HF_TOKEN = 'your_token_here'")
        st.stop()
    
    if data_count == 0:
        st.warning("No jobs found in the vector database. Starting initial seeding...")
        placeholder = st.empty()
        with placeholder.status("Seeding databases (this may take a minute)...", expanded=True) as status:
            st.write("Reading jobs.json...")
            try:
                seed_databases(json_path="jobs.json", db_path="data/jobpulse.db", chroma_dir=CHROMA_DIR)
                st.write("Syncing embeddings to Chroma...")
                status.update(label="Seeding completed!", state="complete", expanded=False)
                st.success("Databases seeded successfully!")
                time.sleep(1) # Give user a moment to see success
                st.rerun()
            except Exception as e:
                status.update(label="Seeding failed!", state="error")
                st.error(f"Seeding failed: {str(e)}")
                st.stop()

check_seed()


def stream_data(text):
    for word in text.split(" "):
        yield word + " "
        time.sleep(0.02)

st.title("JobPulse", width="stretch", anchor=False)
st.caption(f"{data_count} jobs in database")

if "messages" not in st.session_state:
    st.session_state.messages = []

has_message_history = len(st.session_state.messages) > 0

SUGGESTIONS = {
    "Find me jobs that include RAG in job description": ":material/psychology: RAG Engineer",
    "Find me AI Engineer jobs":                        ":material/smart_toy: AI Engineer",
    "Find me Data Engineer jobs":                      ":material/storage: Data Engineer",
    "Find me Software Engineer jobs":                  ":material/code: Software Engineer",
    "Find me Machine Learning Engineer jobs":          ":material/model_training: ML Engineer",
    "Find me Backend Engineer jobs":                   ":material/dns: Backend Engineer",
    "Find me Frontend Engineer jobs":                  ":material/web: Frontend Engineer",
    "Find me DevOps Engineer jobs":                    ":material/cloud_sync: DevOps",
    "Find me Data Scientist jobs":                     ":material/bar_chart: Data Scientist",
    "Find me Product Manager jobs":                    ":material/manage_accounts: Product Manager",
}

# ------ Show pills if no messages -----
if not has_message_history:
    selection = st.pills("", list(SUGGESTIONS.keys()), format_func=lambda x: SUGGESTIONS[x] )

    if selection:
        st.session_state.messages.append({"role": "user", "content": selection})
        st.chat_message("user").markdown(selection)

        response  = llm_service.query(selection)
        st.session_state.messages.append({"role": "ai", "content": response})

        st.rerun()


# ------ handle chat input ------
if prompt := st.chat_input("Say something"):
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    response  = llm_service.query(prompt)
    st.session_state.messages.append({"role": "ai", "content": response})

    st.rerun()

    
for message in st.session_state.messages:
    with st.chat_message(message['role']):
        st.markdown(message["content"])