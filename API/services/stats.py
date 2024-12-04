import json

def get_stats():
    try:
        # Leer datos desde un archivo JSON simulado
        with open("app/data/recommendations.json", "r") as file:
            recommendations = json.load(file)

        # Estadísticas básicas
        advertisers = list(recommendations.keys())
        stats = {
            "total_advertisers": len(advertisers),
            "advertisers_with_most_variations": "adv1, adv2",  # Simular con lógica futura
            "model_agreement": 0.85  # Simulación
        }
        return stats
    except Exception as e:
        return {"error": str(e)}
