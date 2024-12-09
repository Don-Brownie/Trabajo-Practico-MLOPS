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
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Ajustar la consulta según el modelo
        if model == "top_ctr":
            query = """
                SELECT advertiser_id AS product_id, product_id AS advertiser_id, ctr
                FROM top_ctr
                WHERE advertiser_id = %s AND date = CURRENT_DATE
                ORDER BY ctr DESC
            """
        elif model == "top_product":
            query = """
                SELECT advertiser_id, product_id, views
                FROM top_product
                WHERE advertiser_id = %s AND date = CURRENT_DATE
                ORDER BY views DESC
            """
        else:
            return {"error": f"Modelo inválido: {model}"}

        # Ejecutar la consulta
        cursor.execute(query, (adv,))
        results = cursor.fetchall()
        conn.close()

        # Construir las recomendaciones
        recommendations = [{"product_id": row[0], "score": row[2]} for row in results]
        return {"adv": adv, "model": model, "recommendations": recommendations}

    except Exception as e:
        return {"error": str(e)}
