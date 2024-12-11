from app.database.connection import get_connection

async def get_stats():
    """
    Calcula estadísticas sobre las recomendaciones en la base de datos.

    Returns:
        dict: Diccionario con estadísticas clave.
    """
    stats = {}
    queries = {
        # Total de advertisers únicos en ambas tablas
        "total_advertisers": """
            SELECT COUNT(DISTINCT advertiser_id) 
            FROM (
                SELECT advertiser_id FROM top_ctr
                UNION
                SELECT advertiser_id FROM top_product
            ) AS combined_advertisers
        """,
        # Advertiser con más recomendaciones variables por día
        "most_variable_advertisers": """
            SELECT advertiser_id, COUNT(DISTINCT date)
            FROM (
                SELECT advertiser_id, date FROM top_ctr
                UNION ALL
                SELECT advertiser_id, date FROM top_product
            ) AS combined_recommendations
            GROUP BY advertiser_id
            ORDER BY COUNT(DISTINCT date) DESC
            LIMIT 1
        """,
        # Coincidencias entre recomendaciones de ambos modelos por advertiser
        "model_agreement": """
            SELECT t1.advertiser_id, COUNT(*)
            FROM top_ctr AS t1
            INNER JOIN top_product AS t2
            ON t1.product_id = t2.product_id AND t1.advertiser_id = t2.advertiser_id AND t1.date = t2.date
            GROUP BY t1.advertiser_id
            ORDER BY COUNT(*) DESC
        """
    }

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Total Advertisers
        cursor.execute(queries["total_advertisers"])
        row = cursor.fetchone()
        #RealDictRow([('count', 420)])
        stats["total_advertisers"] = row["count"] if row else 0


        # Advertiser con más variaciones diarias en sus recomendaciones
        cursor.execute(queries["most_variable_advertisers"])
        row = cursor.fetchone()
        #RealDictRow([('advertiser_id', '00h07f'), ('count', 1)])
        stats["most_variable_advertisers"] = {
            "advertiser_id": row["advertiser_id"],
            "days": row["count"]
        } if row else {}

        # Coincidencias entre ambos modelos
        cursor.execute(queries["model_agreement"])
        agreements = cursor.fetchall()
        #[]
        stats["model_agreement"] = [
            {"advertiser_id": row["advertiser_id"], "matches": row["count"]}
            for row in agreements
        ]

        conn.close()
        return {"stats": stats}
    except Exception as e:
        return {"error": str(e)}
