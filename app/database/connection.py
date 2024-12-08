import psycopg2
from psycopg2.extras import RealDictCursor

def get_connection():
    return psycopg2.connect(
        host= "grupo-17-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
        database= "grupo-17-rds",
        user= "postgres",
        password= "yourpassword123",
        cursor_factory=RealDictCursor
    )
