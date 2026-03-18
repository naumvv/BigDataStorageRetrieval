WITH target_user AS (
    SELECT
        e.user_id
    FROM events AS e
    WHERE e.user_id IS NOT NULL
    GROUP BY e.user_id
    ORDER BY COUNT(*) DESC, e.user_id
    LIMIT 1
),
user_category_affinity AS (
    SELECT
        tu.user_id AS target_user_id,
        COALESCE(p.category_id, e.category_id) AS category_id,
        COALESCE(p.category_code, e.category_code) AS category_code,
        SUM(
            CASE e.event_type
                WHEN 'purchase' THEN 5
                WHEN 'cart' THEN 3
                WHEN 'view' THEN 1
                WHEN 'remove_from_cart' THEN -1
                ELSE 0
            END
        ) AS category_affinity_score
    FROM target_user AS tu
    JOIN events AS e
      ON e.user_id = tu.user_id
    LEFT JOIN products AS p
      ON p.product_id = e.product_id
    GROUP BY
        tu.user_id,
        COALESCE(p.category_id, e.category_id),
        COALESCE(p.category_code, e.category_code)
    HAVING
        COALESCE(p.category_id, e.category_id) IS NOT NULL
        AND SUM(
            CASE e.event_type
                WHEN 'purchase' THEN 5
                WHEN 'cart' THEN 3
                WHEN 'view' THEN 1
                WHEN 'remove_from_cart' THEN -1
                ELSE 0
            END
        ) > 0
),
purchased_products AS (
    SELECT DISTINCT
        e.product_id
    FROM events AS e
    JOIN target_user AS tu
      ON tu.user_id = e.user_id
    WHERE e.event_type = 'purchase'
),
product_popularity AS (
    SELECT
        e.product_id,
        COALESCE(p.category_id, e.category_id) AS category_id,
        COALESCE(p.category_code, e.category_code) AS category_code,
        MAX(p.brand) AS brand,
        MAX(p.price) AS price,
        SUM(
            CASE e.event_type
                WHEN 'purchase' THEN 5
                WHEN 'cart' THEN 3
                WHEN 'view' THEN 1
                WHEN 'remove_from_cart' THEN -1
                ELSE 0
            END
        ) AS product_popularity_score
    FROM events AS e
    LEFT JOIN products AS p
      ON p.product_id = e.product_id
    GROUP BY
        e.product_id,
        COALESCE(p.category_id, e.category_id),
        COALESCE(p.category_code, e.category_code)
    HAVING
        COALESCE(p.category_id, e.category_id) IS NOT NULL
        AND SUM(
            CASE e.event_type
                WHEN 'purchase' THEN 5
                WHEN 'cart' THEN 3
                WHEN 'view' THEN 1
                WHEN 'remove_from_cart' THEN -1
                ELSE 0
            END
        ) > 0
),
top_recommendations AS (
    SELECT
        uca.target_user_id,
        pp.product_id,
        pp.brand,
        pp.price,
        pp.category_code,
        uca.category_affinity_score,
        pp.product_popularity_score
    FROM user_category_affinity AS uca
    JOIN product_popularity AS pp
      ON pp.category_id = uca.category_id
    LEFT JOIN purchased_products AS purchased
      ON purchased.product_id = pp.product_id
    WHERE purchased.product_id IS NULL
    ORDER BY
        uca.category_affinity_score DESC,
        pp.product_popularity_score DESC,
        pp.product_id
    LIMIT 5
),
search_terms AS (
    SELECT
        target_user_id,
        string_agg(
            DISTINCT trim(regexp_replace(category_code, '[._-]+', ' ', 'g')),
            ' '
        ) AS query_text
    FROM top_recommendations
    WHERE category_code IS NOT NULL
    GROUP BY target_user_id
)
SELECT
    st.target_user_id,
    st.query_text,
    p.product_id,
    p.brand,
    p.price,
    p.category_code,
    ts_rank_cd(
        to_tsvector(
            'simple',
            coalesce(p.category_code, '') || ' ' ||
            regexp_replace(coalesce(p.category_code, ''), '[._-]+', ' ', 'g') || ' ' ||
            coalesce(p.brand, '')
        ),
        websearch_to_tsquery('simple', st.query_text)
    ) AS search_rank
FROM search_terms AS st
JOIN products AS p
  ON to_tsvector(
         'simple',
         coalesce(p.category_code, '') || ' ' ||
         regexp_replace(coalesce(p.category_code, ''), '[._-]+', ' ', 'g') || ' ' ||
         coalesce(p.brand, '')
     ) @@ websearch_to_tsquery('simple', st.query_text)
ORDER BY
    search_rank DESC,
    p.product_id
LIMIT 20;
