from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

MODEL_NAME = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"

device = "mps" if torch.backends.mps.is_available() else "cpu"
dtype = torch.float32  # ✅ Force stable precision

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=dtype)
model.to(device)

def get_llm_response(context: str, query: str):
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
    if "ANSWER:" in response:
        response = response.split("ANSWER:")[-1].strip()
    return response
