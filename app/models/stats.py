from database.connection import get_connection

async def get_stats():
    """
    Calcula estadísticas sobre las recomendaciones en la base de datos.

    Returns:
        dict: Diccionario con estadísticas clave.
    """
    stats = {}
    queries = {
        "total_advertisers": "SELECT COUNT(DISTINCT advertiser_id) FROM recommendations",
        "most_variable_advertisers": """
            SELECT advertiser_id, COUNT(DISTINCT date)
            FROM recommendations
            GROUP BY advertiser_id
            ORDER BY COUNT(DISTINCT date) DESC
            LIMIT 1
        """,
        "model_agreement": """
            SELECT advertiser_id, COUNT(*)
            FROM recommendations AS r1
            INNER JOIN recommendations AS r2
            ON r1.advertiser_id = r2.advertiser_id AND r1.date = r2.date AND r1.product_id = r2.product_id
            WHERE r1.model = 'model_1' AND r2.model = 'model_2'
            GROUP BY advertiser_id
            ORDER BY COUNT(*) DESC
        """
    }

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Total Advertisers
        cursor.execute(queries["total_advertisers"])
        stats["total_advertisers"] = cursor.fetchone()[0]

        # Advertiser with most variable recommendations
        cursor.execute(queries["most_variable_advertisers"])
        row = cursor.fetchone()
        stats["most_variable_advertiser"] = {"advertiser_id": row[0], "days_with_recommendations": row[1]} if row else {}

        # Agreement between models
        cursor.execute(queries["model_agreement"])
        agreements = cursor.fetchall()
        stats["model_agreement"] = [{"advertiser_id": row[0], "agreements": row[1]} for row in agreements]

        conn.close()
        return {"stats": stats}
    except Exception as e:
        return {"error": str(e)}
