from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
from common.logging_config import setup_logger

# ------------------------------------------------
# LOGGER
# ------------------------------------------------
logger = setup_logger("llm_service")

app = FastAPI(
    title="LLM Service",
    description="TinyLlama-powered LLM API",
    version="1.0.0"
)

# ------------------------------------------------
# LOAD MODEL
# ------------------------------------------------
MODEL_NAME = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"

device = "mps" if torch.backends.mps.is_available() else "cpu"
dtype = torch.float32

logger.info(f"🚀 Initializing LLM Service")
logger.info(f"Selected device: {device}")
logger.info(f"Loading model: {MODEL_NAME}")

try:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=dtype
    )
    model.to(device)

    logger.info("✅ LLM model + tokenizer loaded successfully.")

except Exception as e:
    logger.exception(f"❌ Failed to load LLM model: {e}")
    raise


# ------------------------------------------------
# LLM INFERENCE FUNCTION
# ------------------------------------------------
def get_llm_response(context: str, query: str) -> str:
    logger.info(f"Generating response for query: '{query}'")

    try:
        prompt = f"""
You are a financial assistant. 
Read the following relevant market snippets and give a concise, factual answer.

Context:
{context}

Question: {query}

Answer in 2-3 sentences:
"""
        inputs = tokenizer(prompt, return_tensors="pt").to(device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=200,
                temperature=0.7,
                top_p=0.9,
                do_sample=True
            )

        response = tokenizer.decode(outputs[0], skip_special_tokens=True)

        # Clean answer if model adds "Answer:"
        if "Answer:" in response:
            response = response.split("Answer:")[-1].strip()
        elif "ANSWER:" in response:
            response = response.split("ANSWER:")[-1].strip()

        logger.info("LLM generation completed successfully.")
        return response.strip()

    except Exception as e:
        logger.exception(f"❌ Error in LLM generation: {e}")
        return "Error generating LLM response — check logs."


# ------------------------------------------------
# API SCHEMA
# ------------------------------------------------
class LLMRequest(BaseModel):
    context: str
    query: str


class LLMResponse(BaseModel):
    answer: str


# ------------------------------------------------
# ENDPOINTS
# ------------------------------------------------
@app.post("/generate", response_model=LLMResponse)
async def generate(req: LLMRequest):
    logger.info("📩 /generate endpoint hit.")
    logger.info(f"Context size: {len(req.context)} chars | Query: '{req.query}'")

    answer = get_llm_response(req.context, req.query)

    logger.info("📤 Sending LLM response.")
    return LLMResponse(answer=answer)


@app.get("/")
async def root():
    logger.info("Root endpoint accessed.")
    return {"message": "LLM service is running!"}
