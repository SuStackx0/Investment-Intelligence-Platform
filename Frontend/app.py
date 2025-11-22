import streamlit as st
import requests

# -------------------------------
# CONFIG
# -------------------------------
BACKEND_URL = "http://127.0.0.1:8090/ask/"  # note trailing slash

st.set_page_config(page_title="Investment RAG Assistant", page_icon="💹", layout="centered")

st.title("💹 Intelligent Investment Assistant")
st.markdown("Ask any investment or market-related question and get AI-backed insights.")

query = st.text_input("💬 Enter your question:", placeholder="e.g., What is up with Reliance stocks?")

if st.button("Get Insights 🚀") and query.strip():
    with st.spinner("Fetching insights..."):
        try:
            # POST request with JSON payload
            response = requests.post(
                BACKEND_URL,
                json={
                    "query": query,
                    "prefetch_k": 20,
                    "final_k": 5
                },
                timeout=60
            )

            if response.status_code == 200:
                data = response.json()
                st.markdown("---")
                st.subheader("🧠 AI Analysis")
                st.markdown(f"**Question:** {data.get('query', '')}")
                st.write(data.get("answer", "No answer returned."))
                st.markdown("---")
            else:
                st.error(f"❌ Server returned {response.status_code}: {response.text}")

        except Exception as e:
            st.error(f"⚠️ Could not connect to backend: {e}")

else:
    st.info("Type your question above and click 'Get Insights 🚀'")

st.markdown(
    """
    <hr>
    <p style='text-align: center; color: gray;'>
    Built with ❤️ using FastAPI + Streamlit + TinyLlama RAG
    </p>
    """,
    unsafe_allow_html=True,
)
