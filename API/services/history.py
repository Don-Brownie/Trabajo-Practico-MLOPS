from fastapi import HTTPException
import json

def get_history(adv: str):
    try:
        # Leer datos desde un archivo JSON simulado
        with open("app/data/history.json", "r") as file:
            history = json.load(file)
        if adv not in history:
            raise HTTPException(status_code=404, detail="Advertiser no encontrado")
        return {"advertiser": adv, "history": history[adv]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
