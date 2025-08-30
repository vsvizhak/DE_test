/*======================================================
Procedure to load unified to EUR currency orders table
======================================================*/
CREATE OR REPLACE PROCEDURE sp_load_orders_eur()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        INSERT INTO orders_eur (order_id, customer_email, order_date, amount_eur, original_amount, original_currency)
        SELECT
            o.order_id,
            o.customer_email,
            o.order_date,
            ROUND(o.amount * COALESCE(d.rate,1)::numeric,2) AS amount_eur,
            o.amount AS original_amount,
            o.currency AS original_currency
        FROM stg_orders o
        LEFT JOIN orders_eur e
          ON o.order_id = e.order_id
        LEFT JOIN dim_curr_pair d
          ON o.currency = d.base
         AND d.quote = 'EUR'
         AND d.is_current = 1
        WHERE e.order_id IS NULL  -- вибираємо лише ті, яких ще немає в orders_eur
        ON CONFLICT (order_id) DO NOTHING;

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Procedure sp_load_orders_eur failed. SQLSTATE: %, SQLERRM: %',
            SQLSTATE, SQLERRM;
        RAISE;
    END;
END;
$$;
