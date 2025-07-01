from tasks.dbconnect import connect_db

conn = connect_db()
conn.autocommit = True
cursor = conn.cursor()

def execute(sql):
    cursor.execute(sql)

def mart_schema_generate():
    execute('''create schema if not exists mart;''')
    execute_dim_customer()
    execute_dim_store()
    execute_dim_product()
    execute_fact_transaction()
    execute_fact_customer()
    # execute_product_tracking_price_history()
    track_customer_view()
    fact_product_cost_profit()
    track_store_customer_view()
    fact_customer_behavior()
    conn.commit() 
    conn.close() 

def execute_dim_customer():
    # create dim_customer_view
    return execute('''
        CREATE OR REPLACE VIEW mart.dim_customer_view AS
        SELECT DISTINCT
            hub_customer.hk_customer,
            hub_customer.customer_id,
            hub_customer_sat_detail.customer_name
        from vault.hub_customer hub_customer
        inner join vault.hub_customer_sat_detail hub_customer_sat_detail
        on hub_customer.hk_customer = hub_customer_sat_detail.hk_customer
        where hub_customer_sat_detail.is_actived = true;
    ''')

def execute_dim_store():
    # create dim_store_view
    return execute('''
        CREATE OR REPLACE VIEW mart.dim_store_view AS
        SELECT DISTINCT
            hub_store.hk_store,
            hub_store.store_id,
            hub_store_sat_detail.store_name
        FROM vault.hub_store hub_store
        inner join vault.hub_store_sat_detail hub_store_sat_detail
        on hub_store.hk_store = hub_store_sat_detail.hk_store
        where hub_store_sat_detail.is_actived = true;
    ''')

def execute_dim_product():
    # create dim_product_view
    return execute('''
        CREATE OR REPLACE VIEW mart.dim_product_view AS
        SELECT DISTINCT
            hub_product.hk_product,
            hub_product.product_id,
            hub_product_sat_detail.product_name,
            hub_product_sat_detail.category,
            hub_product_sat_detail.price
        FROM vault.hub_product hub_product
        INNER JOIN vault.hub_product_sat_detail hub_product_sat_detail
        on hub_product.hk_product = hub_product_sat_detail.hk_product
        WHERE hub_product_sat_detail.is_actived = true;
    ''')

def execute_fact_transaction():
    # create fact_transaction_view
    return execute('''
        CREATE OR REPLACE VIEW mart.fact_transaction_view AS
        SELECT
            link_transaction_sat_detail.transaction_id,
            link_transaction.hk_transaction,
            link_transaction.hk_customer,
            link_transaction.hk_product,
            link_transaction.hk_store,
            link_transaction_sat_detail.transaction_date,
            link_transaction_sat_detail.total_amount,
            link_transaction_sat_detail.cost,
            link_transaction_sat_detail.profit
        FROM vault.link_transaction link_transaction
        INNER JOIN vault.link_transaction_sat_detail link_transaction_sat_detail
        ON link_transaction.hk_transaction = link_transaction_sat_detail.hk_transaction
        WHERE link_transaction_sat_detail.is_actived = true;
    ''')

def execute_fact_customer():
    return execute('''
        CREATE OR REPLACE VIEW mart.fact_customer_view AS
        SELECT DISTINCT
            hk_customer,
            customer_id,
            customer_name,
            COUNT(transaction_id) AS transaction_count,
            SUM(total_amount) AS total_spent,
            MIN(transaction_date) AS first_transaction_date,
            MAX(transaction_date) AS last_transaction_date
        FROM(
            SELECT DISTINCT
                hub_customer.hk_customer,
                fact_transaction.transaction_id,
                hub_customer.customer_id,
                hub_customer_sat_detail.customer_name,
                fact_transaction.total_amount,
                fact_transaction.transaction_date
            FROM mart.fact_transaction_view fact_transaction
            INNER JOIN vault.hub_customer_sat_detail hub_customer_sat_detail
                ON fact_transaction.hk_customer = hub_customer_sat_detail.hk_customer
            INNER JOIN vault.hub_customer hub_customer
                ON hub_customer.hk_customer = hub_customer_sat_detail.hk_customer
            WHERE hub_customer_sat_detail.is_actived = true
        ) AS SUBQUERY
        GROUP BY 
            hk_customer,
            customer_id,
            customer_name;
    ''')

def track_customer_view():
    return execute('''
        CREATE OR REPLACE VIEW mart.fact_customer_store_view AS
        SELECT
            fact_transaction.hk_store,
            hub_store_sat_detail.store_name,
            fact_transaction.transaction_date,
            COUNT(DISTINCT fact_transaction.hk_customer) AS customer_count
        FROM mart.fact_transaction_view fact_transaction
        INNER JOIN vault.hub_store_sat_detail hub_store_sat_detail
            on fact_transaction.hk_store = hub_store_sat_detail.hk_store
        WHERE hub_store_sat_detail.is_actived = true
        GROUP BY fact_transaction.hk_store, hub_store_sat_detail.store_name, fact_transaction.transaction_date;
    ''')

def fact_product_cost_profit():
    return execute('''
        -- Create the product cost/profit fact table
        CREATE OR REPLACE VIEW mart.fact_product_cost_profit AS
        SELECT DISTINCT
            link_transaction_sat_detail.transaction_id,
            hub_store_sat_detail.hk_store,
            hub_product_sat_detail.hk_product,
            hub_store_sat_detail.store_name,
            hub_product_sat_detail.product_name,
            hub_product_sat_detail.category,
            link_transaction_sat_detail.total_amount,
            link_transaction_sat_detail.cost,
            link_transaction_sat_detail.profit
        FROM vault.link_transaction_sat_detail link_transaction_sat_detail
        INNER JOIN vault.link_transaction link_transaction
            ON link_transaction_sat_detail.hk_transaction = link_transaction.hk_transaction
        INNER JOIN vault.hub_product_sat_detail hub_product_sat_detail
            ON link_transaction.hk_product = hub_product_sat_detail.hk_product
        INNER JOIN vault.hub_store_sat_detail hub_store_sat_detail
            ON link_transaction.hk_store = hub_store_sat_detail.hk_store
        WHERE hub_product_sat_detail.is_actived = True
            AND link_transaction_sat_detail.is_actived = True
            AND hub_store_sat_detail.is_actived = True
    ''')

def track_store_customer_view():
    return execute('''
        CREATE OR REPLACE VIEW mart.track_store_customer AS
        SELECT
            hub_store_sat_detail.store_name,
            DATE_TRUNC('month', ftv.transaction_date::timestamp) AS transaction_month,
            COUNT(DISTINCT ftv.hk_customer) AS monthly_customer_count
        FROM mart.fact_transaction_view ftv
        INNER JOIN vault.hub_store_sat_detail
            ON ftv.hk_store = hub_store_sat_detail.hk_store
        GROUP BY hub_store_sat_detail.store_name, DATE_TRUNC('month', ftv.transaction_date::timestamp)
        ORDER BY store_name, transaction_month;
    ''')

def fact_customer_behavior():
    return execute('''
        CREATE OR REPLACE VIEW mart.fact_customer_behavior AS
        SELECT DISTINCT
            fact_transaction_view.transaction_id,
            fact_customer_view.hk_customer,
            fact_customer_view.customer_id,
            fact_customer_view.customer_name,
            dim_store_view.store_name,
            dim_product_view.product_name,
            fact_transaction_view.total_amount
        FROM mart.fact_customer_view fact_customer_view
        INNER JOIN mart.fact_transaction_view fact_transaction_view
            ON fact_transaction_view.hk_customer = fact_customer_view.hk_customer
        INNER JOIN mart.dim_store_view dim_store_view
            ON fact_transaction_view.hk_store = dim_store_view.hk_store
        INNER JOIN mart.dim_product_view dim_product_view
            ON fact_transaction_view.hk_product = dim_product_view.hk_product
    ''')