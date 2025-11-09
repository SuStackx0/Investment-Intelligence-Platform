from fastapi import FastAPI
from routers.pipeline_router import pipeline_router
from routers.rag_router import rag_router


app=FastAPI(name="Investment Platform Backend")

app.include_router(pipeline_router)
app.include_router(rag_router)

@app.get("/")
async def root():
    return {"message": "Welcome to the Intelligent Investment Platform Backend!"}