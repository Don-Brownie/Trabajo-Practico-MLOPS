from fastapi import FastAPI
from app.models.recommendations import get_recommendations
from app.models.history import get_history
from app.models.stats import get_stats

app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "API funcionando correctamente"}

@app.get("/recommendations/{adv}/{model}")
async def recommendations(adv: str, model: str):
    return await get_recommendations(adv, model)

@app.get("/stats/")
async def stats():
    return await get_stats()

@app.get("/history/{adv}")
async def history(adv: str):
    return await get_history(adv)

@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)