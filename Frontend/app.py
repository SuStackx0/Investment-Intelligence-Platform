import streamlit as st
import requests
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------
BACKEND_URL = "http://127.0.0.1:8090/ask"  # no trailing slash needed

st.set_page_config(
    page_title="Investment RAG Assistant",
    page_icon="💹",
    layout="centered"
)

st.title("💹 Intelligent Investment Assistant")
st.caption("AI-powered RAG system for investment & market insights")

# ---------------------------------
# Session State for Chat History
# ---------------------------------
if "history" not in st.session_state:
    st.session_state.history = []

# ---------------------------------
# User Input
# ---------------------------------
query = st.text_input(
    "💬 Ask a question:",
    placeholder="e.g., What is up with Reliance stocks today?"
)

# ---------------------------------
# Send Request Function
# ---------------------------------
def get_insights(query: str):
    try:
        response = requests.post(
            BACKEND_URL,
            json={"query": query, "prefetch_k": 20, "final_k": 5},
            timeout=60
        )
        if response.status_code == 200:
            return response.json(), None
        else:
            return None, f"Server returned {response.status_code}: {response.text}"
    except Exception as e:
        return None, f"Could not connect to backend: {e}"

# ---------------------------------
# Button Handler
# ---------------------------------
if st.button("Get Insights 🚀", use_container_width=True) and query.strip():
    with st.spinner("Fetching insights…"):
        data, error = get_insights(query)

        timestamp = datetime.now().strftime("%H:%M:%S")
        if error:
            st.error(error)
            st.session_state.history.append(
                {"query": query, "answer": f"❌ {error}", "time": timestamp}
            )
        else:
            answer = data.get("answer", "No answer returned.")
            st.session_state.history.append(
                {"query": query, "answer": answer, "time": timestamp}
            )

# ---------------------------------
# Chat History Renderer
# ---------------------------------
st.markdown("---")
st.subheader("📜 Conversation History")

if len(st.session_state.history) == 0:
    st.info("Ask a question to start the conversation!")
else:
    for item in reversed(st.session_state.history):
        st.markdown(
            f"""
            <div style='background-color:#f8f9fa;padding:12px;border-radius:10px;margin-bottom:12px'>
                <strong>🕒 {item['time']}</strong><br>
                <strong>❓ You:</strong><br> {item['query']}<br><br>
                <strong>🤖 AI:</strong><br> {item['answer']}
            </div>
            """,
            unsafe_allow_html=True
        )

# ---------------------------------
# Footer
# ---------------------------------
st.markdown(
    """
    <hr>
    <p style='text-align: center; color: gray; font-size: 14px;'>
        Built with ❤️ using FastAPI · Streamlit · TinyLlama RAG
    </p>
    """,
    unsafe_allow_html=True,
)
