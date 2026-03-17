CREATE OR REPLACE FUNCTION bd_shops.refresh_daily_shop_visits_mart()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS bd_shops.daily_shop_visits_mart;

    CREATE TABLE bd_shops.daily_shop_visits_mart AS
    SELECT
        visit_date,
        shop_id,
        COUNT(*) AS visits_count,
        ROUND(AVG(line_size)::numeric, 2) AS avg_line_size,
        ROUND(MAX(line_size)::numeric, 2) AS max_line_size
    FROM bd_shops.visits
    GROUP BY visit_date, shop_id;
END;
$$;