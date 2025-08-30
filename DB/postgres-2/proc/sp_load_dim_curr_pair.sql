/*======================================================
Procedure to calculate currency pairs
======================================================*/
CREATE OR REPLACE PROCEDURE sp_load_dim_curr_pair(
    p_load_id   bigint
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- оновлюємо старі записи, де курс змінився
    UPDATE dim_curr_pair d
    SET is_current = 0
    FROM (
        SELECT
               r1.curr_date,
               r1.curr_time,
               r1.curr_code AS base,
               r2.curr_code AS quote,
               COALESCE(r2.rate, 0) / NULLIF(r1.rate, 0) AS new_rate
        FROM stg_currency r1
        CROSS JOIN stg_currency r2
        WHERE r1.curr_code <> r2.curr_code
          AND r1.load_id = p_load_id
          AND r2.load_id = p_load_id
          AND r1.rate IS NOT NULL
          AND r2.rate IS NOT NULL
    ) a
    WHERE d.base = a.base
      AND d.quote = a.quote
      AND d.is_current = 1
      AND d.rate <> a.new_rate;

    -- вставляємо нові або оновлені курси
    INSERT INTO dim_curr_pair(base, quote, rate, is_current)
    SELECT src.base, src.quote, src.new_rate, 1
    FROM (
        SELECT r1.curr_date,
               r1.curr_time,
               r1.curr_code AS base,
               r2.curr_code AS quote,
               COALESCE(r2.rate, 0) / NULLIF(r1.rate, 0) AS new_rate
        FROM stg_currency r1
        CROSS JOIN stg_currency r2
        WHERE r1.curr_code <> r2.curr_code
          AND r1.load_id = p_load_id
          AND r2.load_id = p_load_id
          AND r1.rate IS NOT NULL
          AND r2.rate IS NOT NULL
    ) src
    LEFT JOIN dim_curr_pair d
        ON d.base = src.base
       AND d.quote = src.quote
       AND d.is_current = 1
    WHERE d.base IS NULL OR d.rate <> src.new_rate;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in sp_load_dim_curr_pair with load_id=%: %', p_load_id, SQLERRM;
        RAISE;
END
$$;
