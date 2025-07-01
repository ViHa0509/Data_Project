from tasks.dbconnect import connect_db

def stg0_shema_generate():
    conn = connect_db()

    conn.autocommit = True
    cursor = conn.cursor()

    createSchemaSql = '''create schema if not exists stg0;'''

    cursor.execute(createSchemaSql)

    checkExistTable = '''DROP TABLE IF EXISTS stg0.etl_sale_data;'''
    cursor.execute(checkExistTable)

    sql = '''
        create or replace procedure create_table()
        language plpgsql
        as $$
            begin
                CREATE TABLE stg0.etl_sale_data as
                select
                    MD5( TRIM( LOWER ( stg.customer_id ) ) ) as bk_customer,
                    MD5( CONCAT(
                        TRIM ( LOWER ( stg.customer_id ) ),
                        TRIM ( LOWER ( stg.customer_name ) )
                    ) ) as hk_customer,
                    stg.customer_id,
                    stg.customer_name,
                    stg.email,
                    stg.phone,
                    MD5( TRIM( LOWER ( stg.product_id ) ) ) as bk_product,
                    MD5( CONCAT(
                        TRIM ( LOWER ( stg.product_id ) ),
                        TRIM ( LOWER ( stg.product_name ) )
                    ) ) as hk_product,
                    stg.product_id,
                    stg.product_name,
                    stg.category,
                    stg.price,
                    MD5( TRIM( LOWER ( stg.store_id ) ) ) as bk_store,
                    MD5( CONCAT(
                        TRIM ( LOWER ( stg.store_id ) ),
                        TRIM ( LOWER ( stg.store_name ) )
                    ) ) as hk_store,
                    stg.store_id,
                    stg.store_name,
                    stg.location,
                    MD5( TRIM( LOWER ( stg.transaction_id ) ) ) as bk_transaction,
                    MD5( CONCAT(
                        TRIM ( LOWER ( stg.transaction_id ) ),
                        TRIM ( LOWER ( stg.quantity ) ),
                        TRIM ( LOWER ( stg.transaction_date ) ),
                        TRIM ( LOWER ( stg.total_amount ) ),
                        TRIM ( LOWER ( stg.cost ) ),
                        TRIM ( LOWER ( stg.profit ) )
                    ) ) as hk_transaction,
                    stg.transaction_id,
                    stg.quantity,
                    stg.transaction_date,
                    stg.total_amount,
                    stg.cost,
                    stg.profit
                FROM stg.etl_sale_data stg;
            end;
        $$;

        call create_table(); 
    '''

    cursor.execute(sql)

    conn.commit() 
    conn.close() 