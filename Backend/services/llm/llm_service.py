# services/llm/llm_service.py
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch


MODEL_NAME = "microsoft/phi-2"  


device = "mps" if torch.backends.mps.is_available() else "cpu"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.float16 if device == "mps" else torch.float32)
model.to(device)

def get_llm_response(context: str, query: str):
    """Generate response using a small local model running on M2."""
    prompt = f"""
    You are an investment assistant.
    Use the context below to answer the question precisely.

    CONTEXT:
    {context}

    QUESTION:
    {query}

    ANSWER:
    """

    inputs = tokenizer(prompt, return_tensors="pt").to(device)
    outputs = model.generate(
        **inputs,
        max_new_tokens=200,
        temperature=0.4,
        do_sample=True,
        top_p=0.9,
    )

    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    if "ANSWER:" in response:
        response = response.split("ANSWER:")[-1].strip()
    return response
