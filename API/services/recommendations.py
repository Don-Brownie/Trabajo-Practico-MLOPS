from fastapi import HTTPException
import json

def get_recommendations(adv: str, model: str):
    try:
        # Leer datos desde un archivo JSON simulado
        with open("app/data/recommendations.json", "r") as file:
            recommendations = json.load(file)
        if adv not in recommendations or model not in recommendations[adv]:
            raise HTTPException(status_code=404, detail="Advertiser o modelo no encontrado")
        return {"advertiser": adv, "model": model, "recommendations": recommendations[adv][model]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
