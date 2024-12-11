from app.database.connection import get_connection

async def get_history(adv: str):
    """
    Obtiene el historial de recomendaciones de los últimos 7 días para un advertiser.

    Args:
        adv (str): Advertiser ID.

    Returns:
        dict o list:
            - Una lista de objetos con las recomendaciones si se encuentran.
            - Un objeto con el campo "error" en caso de no encontrar datos o haber un error.
    """
    query = """
        SELECT date, product_id, 'top_ctr' AS source
        FROM top_ctr
        WHERE advertiser_id = %s AND date >= CURRENT_DATE - INTERVAL '7 days'
        UNION ALL
        SELECT date, product_id, 'top_product' AS source
        FROM top_product
        WHERE advertiser_id = %s AND date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY date DESC
    """

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Ejecutar la consulta con el advertiser ID
        cursor.execute(query, (adv, adv))
        history = cursor.fetchall()
        conn.close()

        # Si no hay resultados, devolver un arreglo vacío
        if not history:
            return []  # Se devuelve una lista vacía si no hay resultados

        # Formatear los resultados
        formatted_history = [
            {"date": str(row[0]), "product_id": row[1], "source": row[2]}
            for row in history
        ]

        return formatted_history

    except Exception as e:
        return {"error": str(e)}
