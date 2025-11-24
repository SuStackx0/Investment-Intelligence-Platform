import streamlit as st
import requests
from datetime import datetime
from typing import Dict, Optional, Tuple
import json

# -------------------------------
# CONFIG
# -------------------------------
BACKEND_URL = "http://127.0.0.1:8090/ask"
REQUEST_TIMEOUT = 60
DEFAULT_PREFETCH_K = 20
DEFAULT_FINAL_K = 5

# -------------------------------
# PAGE CONFIG
# -------------------------------
st.set_page_config(
    page_title="Investment RAG Assistant",
    page_icon="💹",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# -------------------------------
# CUSTOM CSS
# -------------------------------
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .stTextInput > div > div > input {
        font-size: 16px;
    }
    .chat-message {
        background-color: #f8f9fa;
        padding: 16px;
        border-radius: 12px;
        margin-bottom: 16px;
        border-left: 4px solid #4CAF50;
    }
    .error-message {
        border-left: 4px solid #f44336;
    }
    .timestamp {
        color: #666;
        font-size: 12px;
        margin-bottom: 8px;
    }
    .query-text {
        color: #1976D2;
        font-weight: 500;
        margin-bottom: 8px;
    }
    .answer-text {
        color: #333;
        line-height: 1.6;
    }
    </style>
""", unsafe_allow_html=True)

# -------------------------------
# SESSION STATE INITIALIZATION
# -------------------------------
if "history" not in st.session_state:
    st.session_state.history = []
if "loading" not in st.session_state:
    st.session_state.loading = False

# -------------------------------
# HEADER
# -------------------------------
st.title("💹 Intelligent Investment Assistant")
st.caption("AI-powered RAG system for investment & market insights")

# -------------------------------
# SIDEBAR CONFIGURATION
# -------------------------------
with st.sidebar:
    st.header("⚙️ Settings")
    
    backend_url = st.text_input(
        "Backend URL",
        value=BACKEND_URL,
        help="URL of the RAG backend service"
    )
    
    prefetch_k = st.slider(
        "Prefetch K",
        min_value=5,
        max_value=50,
        value=DEFAULT_PREFETCH_K,
        help="Number of documents to initially retrieve"
    )
    
    final_k = st.slider(
        "Final K",
        min_value=1,
        max_value=20,
        value=DEFAULT_FINAL_K,
        help="Number of documents to use for final answer"
    )
    
    st.markdown("---")
    
    if st.button("🗑️ Clear History", use_container_width=True):
        st.session_state.history = []
        st.rerun()
    
    st.markdown("---")
    st.markdown("### 💡 Sample Questions")
    st.markdown("""
    - What is up with Reliance stocks today?
    - Compare tech sector performance
    - Latest market trends in banking
    - Should I invest in renewable energy?
    """)

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------
def validate_input(query: str) -> Tuple[bool, Optional[str]]:
    """Validate user input"""
    if not query or not query.strip():
        return False, "Please enter a question."
    if len(query.strip()) < 3:
        return False, "Question is too short. Please be more specific."
    if len(query) > 500:
        return False, "Question is too long. Please keep it under 500 characters."
    return True, None

def get_insights(query: str, url: str, prefetch: int, final: int) -> Tuple[Optional[Dict], Optional[str]]:
    """Fetch insights from backend API"""
    try:
        payload = {
            "query": query.strip(),
            "prefetch_k": prefetch,
            "final_k": final
        }
        
        response = requests.post(
            url,
            json=payload,
            timeout=REQUEST_TIMEOUT,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            return response.json(), None
        elif response.status_code == 404:
            return None, "Backend service not found. Please check the URL."
        elif response.status_code == 500:
            return None, "Backend server error. Please try again later."
        else:
            error_detail = response.text[:200] if response.text else "Unknown error"
            return None, f"Server error ({response.status_code}): {error_detail}"
            
    except requests.exceptions.Timeout:
        return None, "Request timed out. Please try again."
    except requests.exceptions.ConnectionError:
        return None, f"Could not connect to backend at {url}. Is the service running?"
    except requests.exceptions.RequestException as e:
        return None, f"Request failed: {str(e)}"
    except json.JSONDecodeError:
        return None, "Invalid response from server."
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"

def render_chat_message(item: Dict, is_error: bool = False):
    """Render a single chat message"""
    error_class = "error-message" if is_error else ""
    icon = "❌" if is_error else "🤖"
    
    st.markdown(
        f"""
        <div class='chat-message {error_class}'>
            <div class='timestamp'>🕒 {item['time']}</div>
            <div class='query-text'>❓ You: {item['query']}</div>
            <div class='answer-text'>{icon} AI: {item['answer']}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# -------------------------------
# MAIN INPUT AREA
# -------------------------------
col1, col2 = st.columns([4, 1])

with col1:
    query = st.text_input(
        "💬 Ask a question:",
        placeholder="e.g., What is up with Reliance stocks today?",
        label_visibility="collapsed",
        key="query_input"
    )

with col2:
    submit_button = st.button(
        "Ask 🚀",
        use_container_width=True,
        type="primary",
        disabled=st.session_state.loading
    )

# -------------------------------
# QUERY PROCESSING
# -------------------------------
if submit_button and query:
    # Validate input
    is_valid, error_msg = validate_input(query)
    
    if not is_valid:
        st.warning(error_msg)
    else:
        st.session_state.loading = True
        
        with st.spinner("🔍 Analyzing your question and fetching insights..."):
            data, error = get_insights(
                query=query,
                url=backend_url,
                prefetch=prefetch_k,
                final=final_k
            )
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            if error:
                st.error(f"❌ {error}")
                st.session_state.history.append({
                    "query": query,
                    "answer": error,
                    "time": timestamp,
                    "is_error": True
                })
            else:
                answer = data.get("answer", "No answer returned from the system.")
                sources = data.get("sources", [])
                
                # Display answer
                st.success("✅ Answer retrieved successfully!")
                st.markdown("### 📊 Answer:")
                st.markdown(answer)
                
                # Display sources if available
                if sources:
                    with st.expander("📚 View Sources"):
                        for idx, source in enumerate(sources, 1):
                            st.markdown(f"**Source {idx}:**")
                            st.text(source)
                            st.markdown("---")
                
                # Add to history
                st.session_state.history.append({
                    "query": query,
                    "answer": answer,
                    "time": timestamp,
                    "is_error": False,
                    "sources": sources
                })
        
        st.session_state.loading = False

# -------------------------------
# CONVERSATION HISTORY
# -------------------------------
st.markdown("---")
st.subheader("📜 Conversation History")

if not st.session_state.history:
    st.info("💡 Ask a question to start the conversation!")
else:
    for item in reversed(st.session_state.history):
        render_chat_message(item, is_error=item.get("is_error", False))
        
        # Add sources expander for successful queries
        if not item.get("is_error", False) and item.get("sources"):
            with st.expander(f"📚 Sources for query at {item['time']}"):
                for idx, source in enumerate(item["sources"], 1):
                    st.markdown(f"**Source {idx}:**")
                    st.text(source)
                    if idx < len(item["sources"]):
                        st.markdown("---")

# -------------------------------
# FOOTER
# -------------------------------
st.markdown("---")
st.markdown(
    """
    <p style='text-align: center; color: gray; font-size: 14px;'>
        Built with ❤️ using FastAPI · Streamlit · TinyLlama RAG<br>
        <em>Powered by Retrieval-Augmented Generation</em>
    </p>
    """,
    unsafe_allow_html=True
)