from app.database.connection import get_connection

async def get_recommendations(adv: str, model: str):
    """
    Obtiene las recomendaciones del día para un advertiser (adv) y modelo específico.

    Args:
        adv (str): Advertiser ID.
        model (str): Modelo a usar para las recomendaciones.

    Returns:
        dict: Diccionario con las recomendaciones.
    """
    query = """
        SELECT product_id, score
        FROM recommendations
        WHERE advertiser_id = %s AND model = %s AND date = CURRENT_DATE
        ORDER BY score DESC
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (adv, model))
        results = cursor.fetchall()
        conn.close()

        recommendations = [{"product_id": row[0], "score": row[1]} for row in results]
        return {"adv": adv, "model": model, "recommendations": recommendations}
    except Exception as e:
        return {"error": str(e)}
