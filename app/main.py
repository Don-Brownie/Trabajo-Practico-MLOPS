from fastapi import FastAPI, HTTPException
from app.models.recommendations import get_recommendations
from app.models.history import get_history
from app.models.stats import get_stats
app = FastAPI()

@app.get("/recommendations/{adv}/{model}")
async def recommendations(adv: str, model: str):
    try:
        result = get_recommendations(adv, model)
        return {"adv": adv, "model": model, "recommendations": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/stats/")
async def stats():
    try:
        result = get_stats()
        return {"stats": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error calculating stats")

@app.get("/history/{adv}")
async def history(adv: str):
    try:
        result = get_history(adv)
        return {"adv": adv, "history": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
