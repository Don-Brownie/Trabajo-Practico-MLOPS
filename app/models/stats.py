from database.connection import get_db_connection

def get_stats():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        # Total advertisers
        cursor.execute("SELECT COUNT(DISTINCT advertiser_id) FROM recommendations")
        total_advertisers = cursor.fetchone()[0]

        # Advertisers with most changes in recommendations
        cursor.execute("""
            SELECT advertiser_id, COUNT(DISTINCT date) AS changes
            FROM recommendations
            GROUP BY advertiser_id
            ORDER BY changes DESC
            LIMIT 5
        """)
        advertisers_changes = cursor.fetchall()

        # Coincidence between TopCTR and TopProduct
        cursor.execute("""
            SELECT COUNT(*)
            FROM TopCTR_recommendations t1
            INNER JOIN TopProduct_recommendations t2
            ON t1.advertiser_id = t2.advertiser_id
               AND t1.product_id = t2.product_id
        """)
        coincidence = cursor.fetchone()[0]

    return {
        "total_advertisers": total_advertisers,
        "top_changes": [{"advertiser_id": row[0], "changes": row[1]} for row in advertisers_changes],
        "model_coincidences": coincidence
    }
