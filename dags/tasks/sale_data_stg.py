import os
from tasks.dbconnect import connect_db
import io
import json
import pandas as pd

# current_dir = os.path.dirname(os.path.abspath(__file__))

def stg_shema_generate(csv_buffer):
    conn = connect_db()

    conn.autocommit = True
    cursor = conn.cursor()

    createSchemaSql = '''create schema if not exists stg;'''
    
    cursor.execute(createSchemaSql)

    checkExistTable = '''DROP TABLE IF EXISTS stg.etl_sale_data;'''
    cursor.execute(checkExistTable)

    createTableSql = '''
        CREATE TABLE stg.etl_sale_data(
            transaction_id varchar(100),
            customer_id varchar(100),
            customer_name varchar(100),
            email varchar(100),
            phone bigint,
            product_id varchar(100),
            product_name varchar(100),
            category varchar(100),
            price float,
            store_id varchar(100),
            store_name varchar(100),
            location varchar(100),
            quantity varchar(100),
            transaction_date varchar(100),
            total_amount varchar(100),
            cost varchar(100),
            profit varchar(100)
        );
    '''

    cursor.execute(createTableSql)

    # Corrected COPY statement
    copySql = '''
        COPY stg.etl_sale_data(
            transaction_id, customer_id, customer_name,
            email, phone, product_id,
            product_name, category, price,
            store_id, store_name, location,
            quantity, transaction_date, total_amount,
            cost, profit
        )
        FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    '''
    cursor.copy_expert(copySql, csv_buffer)

    conn.commit()
    cursor.close() 
    conn.close() 