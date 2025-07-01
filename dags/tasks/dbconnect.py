import psycopg2

def connect_db():
    return psycopg2.connect(
        database="airflow", user='airflow', password='airflow',
        host='172.20.0.2', port='5432',
)