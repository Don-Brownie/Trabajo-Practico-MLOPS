from fastapi import FastAPI
from services.recommendations import get_recommendations
from services.stats import get_stats
from services.history import get_history

app = FastAPI(
    title="AdTech API",
    description="API para manejar recomendaciones y estadísticas de ads",
    version="1.0.0",
)

@app.get("/recommendations/{adv}/{model}")
async def recommendations(adv: str, model: str):
    """
    Endpoint para obtener recomendaciones del día para un advertiser y modelo.
    """
    return get_recommendations(adv, model)

@app.get("/stats/")
async def stats():
    """
    Endpoint para obtener estadísticas generales.
    """
    return get_stats()

@app.get("/history/{adv}")
async def history(adv: str):
    """
    Endpoint para obtener el historial de recomendaciones de los últimos 7 días.
    """
    return get_history(adv)
