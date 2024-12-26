import time
import pandas as pd
from sqlalchemy import create_engine
import psycopg2   # pip install psycopg2
import csv
# Give your connection string here
conn_string = 'postgresql://postgres:password@localhost:5432/database'
#Establish connection
pg_conn = psycopg2.connect(conn_string)
cur = pg_conn.cursor()
start_time = time.time()
print("to_sql duration: {} seconds".format(time.time() - start_time))

def ingest_data():

    #Commit and close the connection
    sql = '''
    COPY tablename
    FROM '{filepath}/test.csv'
    DELIMITER ',' CSV;
    '''
    start_time = time.time()
    df.to_csv('test.csv', index=False, header=False) #Name the .csv file reference in line 29 here
    cur.execute(sql)

    pg_conn.commit()
    cur.close()
    pg_conn.close()
    print("COPY duration: {} seconds".format(time.time() - start_time))
    print("Data ingested successfully")

if __name__ == "__main__":
    ingest_data()
