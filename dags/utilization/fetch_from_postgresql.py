import pandas as pd
import psycopg2 as db

def FetchFromPostgresql():
    # Connect to PostgreSQL
    conn_string = "dbname='house_prediction_db' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    
    # Fetch data from PostgreSQL
    df = pd.read_sql("SELECT * FROM house_prediction_table;", conn)
    conn.close()

    # Save to a CSV file
    df.to_csv('/opt/airflow/data/data_cleaned.csv', index=False)

if __name__ == "__main__":
    FetchFromPostgresql()