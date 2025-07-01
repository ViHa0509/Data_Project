from tasks.dbconnect import connect_db
conn = connect_db()
conn.autocommit = True
cursor = conn.cursor()

def execute(sql):
    cursor.execute(sql)

def vault_schema_generate():
    execute('''create schema if not exists vault;''')
    #execute customer and satilite detail customer
    hub_customer()
    hub_customer_statilite()

    #execute product and satilite detail product
    hub_product()
    hub_product_satilite()

    #execute store and satilite detail store
    hub_store()
    hub_store_satilite()

    #execute transaction and satilite detail transaction
    link_transaction()
    link_transaction_satilite()

    #commit transaction
    conn.commit() 
    conn.close() 

def hub_customer():
    # Hub Customer
    return execute('''
        create or replace procedure hub_customer_procedure()
        language plpgsql
        as $$
            begin
                create table if not exists vault.hub_customer(
                    bk_customer text,
                    hk_customer text primary key,
                    customer_id text,
                    load_date timestamp
                );

                insert into vault.hub_customer(bk_customer, hk_customer, customer_id, load_date)
                select distinct 
                    stg0.bk_customer, 
                    stg0.hk_customer,
                    stg0.customer_id, 
                    current_timestamp(0)
                from stg0.etl_sale_data stg0
                on conflict(hk_customer) do nothing; --skip existing records
            end;
        $$;
            
        call hub_customer_procedure(); 
    ''')

def hub_customer_statilite():
    # Hub Customer sat detail
    return execute('''
        create or replace procedure hub_customer_sat_detail_procedure()
        language plpgsql
        as $$
            begin
                create table if not exists vault.hub_customer_sat_detail(
                    hk_customer text,
                    customer_name text,
                    email text,
                    phone text,
                    is_actived boolean,
                    load_date timestamp,
                    end_date timestamp default null, -- Track deactivation time
                    hash_diff text
                );
                -- Deactivate old records if hash_diff is different and set end_date is current date
                update vault.hub_customer_sat_detail h
                set is_actived = False,
                    end_date = current_timestamp(0) -- Set end_date when deactivating
                where exists (
                    select 1
                    from stg0.etl_sale_data s
                    where h.hk_customer = s.hk_customer
                    and h.hash_diff <> MD5(CONCAT(
                        s.hk_customer,
                        TRIM(LOWER(s.customer_name)), 
                        TRIM(LOWER(s.email)), 
                        TRIM(LOWER(s.phone::TEXT))
                    ))
                ) and h.is_actived = True;

                --Insert only new records with distinct hash_diff
                insert into vault.hub_customer_sat_detail (
                    hk_customer, customer_name, email, phone, is_actived, load_date, end_date, hash_diff
                )
                select distinct
                    stg0.hk_customer,
                    stg0.customer_name,
                    stg0.email,
                    stg0.phone,
                    True,
                    current_timestamp(0),
                    NULL::TIMESTAMP as end_date,
                    MD5(CONCAT(
                        stg0.hk_customer, 
                        TRIM(LOWER(stg0.customer_name)), 
                        TRIM(LOWER(stg0.email)), 
                        TRIM(LOWER(stg0.phone::TEXT))
                    ))
                from stg0.etl_sale_data stg0
                where not exists (
                    select 1
                    from vault.hub_customer_sat_detail h
                    where h.hk_customer = stg0.hk_customer
                        and h.is_actived = True
                        and h.hash_diff = MD5(CONCAT(
                            stg0.hk_customer, 
                            TRIM(LOWER(stg0.customer_name)), 
                            TRIM(LOWER(stg0.email)), 
                            TRIM(LOWER(stg0.phone::TEXT))
                        )
                    )
                );
            end;
        $$;
            
        call hub_customer_sat_detail_procedure(); 
    ''')

def hub_product():
    # Hub product
    return execute('''
        create or replace procedure hub_product_procedure()
        language plpgsql
        as $$
            begin
                create table if not exists vault.hub_product(
                    bk_product text,
                    hk_product text primary key,
                    product_id text,
                    load_date timestamp
                );

                insert into vault.hub_product(bk_product, hk_product, product_id, load_date)
                select distinct
                    stg0.bk_product,
                    stg0.hk_product,
                    stg0.product_id,
                    current_timestamp(0)
                from stg0.etl_sale_data stg0
                on conflict(hk_product) do nothing; --skip existing records
            end;
        $$;
            
        call hub_product_procedure(); 
    ''')

def hub_product_satilite():
    # Hub product sat detail
    return execute('''
        create or replace procedure hub_product_sat_detail_procedure()
        language plpgsql
        as $$
            begin
                -- Ensure table exists with end_date column
                create table if not exists vault.hub_product_sat_detail (
                    hk_product text,
                    product_name text,
                    category text,
                    price NUMERIC(10,2),
                    is_actived boolean,
                    load_date timestamp,
                    end_date timestamp default null, -- Track deactivation time
                    hash_diff text
                );

                -- Step 1: Deactivate old records by setting is_actived = False and updating end_date
                update vault.hub_product_sat_detail h
                set is_actived = False,
                    end_date = current_timestamp(0) -- Set end_date when deactivating
                where exists (
                    select 1 
                    from stg0.etl_sale_data stg0
                    where h.hk_product = stg0.hk_product
                    and h.hash_diff <> MD5(CONCAT(
                        stg0.hk_product, 
                        TRIM(LOWER(stg0.product_name)), 
                        TRIM(LOWER(stg0.category)), 
                        stg0.price
                    ))
                ) and h.is_actived = True;

                -- Step 2: Insert only new records with distinct hash_diff and set end_date = NULL
                insert into vault.hub_product_sat_detail (
                    hk_product, product_name, category, price, is_actived, load_date, end_date, hash_diff
                )
                select distinct
                    stg0.hk_product,
                    stg0.product_name,
                    stg0.category,
                    stg0.price,
                    True as is_actived,
                    current_timestamp(0) as load_date,
                    NULL::TIMESTAMP as end_date, -- New records have no end_date initially
                    MD5(CONCAT(
                        stg0.hk_product, 
                        TRIM(LOWER(stg0.product_name)), 
                        TRIM(LOWER(stg0.category)), 
                        stg0.price
                    )) as hash_diff
                from stg0.etl_sale_data stg0
                where not exists (
                    select 1 
                    from vault.hub_product_sat_detail h 
                    where h.hk_product = stg0.hk_product 
                        and h.is_actived = True
                        and h.hash_diff = MD5(CONCAT(
                            stg0.hk_product, 
                            TRIM(LOWER(COALESCE(stg0.product_name, ''))), 
                            TRIM(LOWER(COALESCE(stg0.category, ''))), 
                            COALESCE(stg0.price, 0)
                        )
                    )
                );
            end;
        $$;
            
        call hub_product_sat_detail_procedure(); 
    ''')

def hub_store():
    # Hub store
    return execute('''
        create or replace procedure hub_store_procedure()
        language plpgsql
        as $$
            begin
                create table if not exists vault.hub_store(
                    bk_store text,
                    hk_store text primary key,
                    store_id text,
                    load_date timestamp
                );

                insert into vault.hub_store (bk_store, hk_store, store_id, load_date)
                select distinct
                    stg0.bk_store,
                    stg0.hk_store,
                    stg0.store_id,
                    current_timestamp(0) as load_date
                from stg0.etl_sale_data stg0
                on conflict(hk_store) do nothing; --skip existing records
            end;
        $$;
            
        call hub_store_procedure(); 
    ''')

def hub_store_satilite():
    # Hub store sat detail
    return execute('''
        create or replace procedure hub_store_sat_detail_procedure()
        language plpgsql
        as $$
        begin
            -- Ensure table exists with end_date column
            create table if not exists vault.hub_store_sat_detail (
                hk_store text,
                store_name text,
                location text,
                is_actived boolean,
                load_date timestamp,
                end_date timestamp default null, -- Track deactivation time
                hash_diff text
            );

            -- Step 1: Deactivate old records by setting is_actived = False and updating end_date
            update vault.hub_store_sat_detail h
            set is_actived = False,
                end_date = current_timestamp(0) -- Set end_date when deactivating
            where exists (
                select 1 
                from stg0.etl_sale_data s
                where h.hk_store = s.hk_store
                and h.hash_diff <> MD5(CONCAT(
                    s.hk_store, 
                    TRIM(LOWER(s.store_name)), 
                    TRIM(LOWER(s.location))
                ))
            ) and h.is_actived = True;

            -- Step 2: Insert only new records with distinct hash_diff and set end_date = NULL
            insert into vault.hub_store_sat_detail (
                hk_store, store_name, location, is_actived, load_date, end_date, hash_diff
            )
            select distinct
                stg0.hk_store,
                stg0.store_name,
                stg0.location,
                True as is_actived,
                current_timestamp(0) as load_date,
                NULL::TIMESTAMP as end_date, -- New records have no end_date initially
                MD5(CONCAT(
                    stg0.hk_store, 
                    TRIM(LOWER(stg0.store_name)), 
                    TRIM(LOWER(stg0.location))
                )) as hash_diff
            from stg0.etl_sale_data stg0
            where not exists (
                select 1 
                from vault.hub_store_sat_detail h 
                where h.hk_store = stg0.hk_store
                    and h.is_actived = True
                    and h.hash_diff = MD5(CONCAT(
                        stg0.hk_store, 
                        TRIM(LOWER(stg0.store_name)), 
                        TRIM(LOWER(stg0.location))
                    ))
            );
        end;
        $$;

        call hub_store_sat_detail_procedure();
    ''')

def link_transaction():
    # Link transaction
    return execute('''
        create or replace procedure link_transaction_procedure()
        language plpgsql
        as $$
            begin
                create table if not exists vault.link_transaction(
                    bk_transaction text,
                    hk_transaction text primary key,
                    hk_customer text,
                    hk_product text,
                    hk_store text,
                    load_date timestamp
                );
                
                insert into  vault.link_transaction(bk_transaction, hk_transaction, hk_customer, hk_product, hk_store, load_date)
                select distinct
                    stg0.bk_transaction,
                    stg0.hk_transaction,
                    stg0.hk_customer,
                    stg0.hk_product,
                    stg0.hk_store,
                    current_timestamp(0) as load_date
                from stg0.etl_sale_data stg0
                on conflict(hk_transaction) do nothing; --skip existing records
            end;
        $$;
            
        call link_transaction_procedure(); 
    ''')

def link_transaction_satilite():
    # Link transaction sat detail
    return execute('''
        create or replace procedure link_transaction_sat_detail_procedure()
        language plpgsql
        as $$
        begin
            -- Ensure table exists with end_date column
            create table if not exists vault.link_transaction_sat_detail (
                hk_transaction text,
                transaction_id text,
                quantity text,
                price numeric,
                transaction_date text,
                total_amount numeric,
                cost numeric,
                profit numeric,
                is_actived boolean,
                load_date timestamp,
                end_date timestamp default null, -- Track deactivation time
                hash_diff text
            );
            -- Step 1: Deactivate old records by setting is_actived = False and updating end_date
            update vault.link_transaction_sat_detail h
            set is_actived = False,
                end_date = current_timestamp(0) -- Set end_date when deactivating
            where exists (
                select 1 
                from stg0.etl_sale_data stg0
                where h.transaction_id = stg0.transaction_id
                and h.hash_diff <> MD5(CONCAT(
                    stg0.hk_transaction, 
                    stg0.transaction_id, 
                    stg0.quantity,
                    stg0.price,
                    stg0.transaction_date, 
                    stg0.total_amount::numeric,
                    stg0.cost::numeric,
                    stg0.profit::numeric
                ))
            ) and h.is_actived = True;

            -- Step 2: Insert only new records with distinct hash_diff and set end_date = NULL
            insert into vault.link_transaction_sat_detail (
                hk_transaction, transaction_id, quantity, price, transaction_date, total_amount, 
                cost, profit, is_actived, load_date, end_date, hash_diff
            )
            select distinct
                stg0.hk_transaction,
                stg0.transaction_id,
                stg0.quantity,
                stg0.price,
                stg0.transaction_date,
                stg0.total_amount::numeric,
                stg0.cost::numeric,
                stg0.profit::numeric,
                True as is_actived,
                current_timestamp(0) as load_date,
                NULL::TIMESTAMP as end_date, -- New records have no end_date initially
                MD5(CONCAT(
                    stg0.hk_transaction, 
                    stg0.transaction_id, 
                    stg0.quantity,
                    stg0.price,
                    stg0.transaction_date, 
                    stg0.total_amount::numeric,
                    stg0.cost::numeric,
                    stg0.profit::numeric
                )) as hash_diff
            from stg0.etl_sale_data stg0
            where not exists (
                select 1 
                from vault.link_transaction_sat_detail h 
                where h.hk_transaction = stg0.hk_transaction
                    and h.is_actived = True
                    and h.hash_diff = MD5(CONCAT(
                        stg0.hk_transaction, 
                        stg0.transaction_id, 
                        stg0.quantity,
                        stg0.price, 
                        stg0.transaction_date, 
                        stg0.total_amount::numeric,
                        stg0.cost::numeric,
                        stg0.profit::numeric
                    )
                )
            );
        end;
        $$;

        call link_transaction_sat_detail_procedure();
    ''') 