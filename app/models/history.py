from app.database.connection import get_db_connection

def get_history(adv):
    conn = get_db_connection()
    query = """
        SELECT date, product_id, model
        FROM recommendations
        WHERE advertiser_id = %s AND date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY date DESC
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (adv,))
        history = cursor.fetchall()
    if not history:
        raise ValueError("No history found for the given advertiser.")
    return [{"date": row[0], "product_id": row[1], "model": row[2]} for row in history]
