from app.database.connection import get_connection
from psycopg2.extras import RealDictCursor

async def get_history(adv: str):
    """
    Obtiene el historial de recomendaciones de los últimos 7 días para un advertiser.

    Args:
        adv (str): Advertiser ID.

    Returns:
        dict: Objeto JSON con "adv" y "history".
              Si no hay datos, "history" será una lista vacía.
              Si hay un error, devolverá {"error": "<mensaje>"}
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
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, (adv, adv))
        results = cursor.fetchall()
        conn.close()

        # Convertir cada fila en dict
        history = [dict(row) for row in results] if results else []

        return {
            "adv": adv,
            "history": history
        }

    except Exception as e:
        return {"error": str(e)}
