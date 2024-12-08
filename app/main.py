from fastapi import FastAPI
from app.models.recommendations import get_recommendations
from app.models.history import get_history
from app.models.stats import get_stats

app = FastAPI()

@app.get("/recommendations/{adv}/{model}")
async def recommendations(adv: str, model: str):
    return await get_recommendations(adv, model)

@app.get("/stats/")
async def stats():
    return await get_stats()

@app.get("/history/{adv}")
async def history(adv: str):
    return await get_history(adv)
