from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

app = FastAPI(
    title="LLM Service",
    description="TinyLlama-powered LLM API",
    version="1.0.0"
)

# -----------------------------
# LOAD MODEL
# -----------------------------
MODEL_NAME = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"

device = "mps" if torch.backends.mps.is_available() else "cpu"
dtype = torch.float32  # stable precision for MPS

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    torch_dtype=dtype
)
model.to(device)


# -----------------------------
# LLM FUNCTION
# -----------------------------
def get_llm_response(context: str, query: str) -> str:
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

    return response.strip()


# -----------------------------
# API SCHEMA
# -----------------------------
class LLMRequest(BaseModel):
    context: str
    query: str


class LLMResponse(BaseModel):
    answer: str


# -----------------------------
# ENDPOINT
# -----------------------------
@app.post("/generate", response_model=LLMResponse)
async def generate(req: LLMRequest):
    answer = get_llm_response(req.context, req.query)
    return LLMResponse(answer=answer)


@app.get("/")
async def root():
    return {"message": "LLM service is running!"}
