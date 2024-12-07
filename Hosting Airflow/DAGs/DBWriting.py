from sqlalchemy import create_engine

def run_db_writing():
    # Configurar conexión a PostgreSQL
    engine = create_engine('postgresql+psycopg2://admin:mypassword123@grupo-17-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com:5432/mydatabase')


    # Leer los resultados desde EC2
    top_ctr = pd.read_csv('/tmp/top_ctr.csv')
    top_products = pd.read_csv('/tmp/top_products.csv')

    # Escribir en la base de datos
    with engine.connect() as conn:
        top_ctr.to_sql('top_ctr', con=conn, if_exists='replace', index=False)
        top_products.to_sql('top_products', con=conn, if_exists='replace', index=False)
