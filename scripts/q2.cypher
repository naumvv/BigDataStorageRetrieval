
// q2.cypher
// Recommend top personalized products for a deterministic target user.

CALL {
    MATCH (u:User)-[e:INTERACTED_WITH]->(:Product)
    RETURN u.user_id AS target_user_id, count(e) AS event_count
    ORDER BY event_count DESC, target_user_id ASC
    LIMIT 1
}
MATCH (target:User {user_id: target_user_id})
CALL {
    WITH target
    MATCH (target)-[e:INTERACTED_WITH]->(p:Product)
    WITH
        coalesce(p.category_id, e.category_id) AS category_id,
        coalesce(p.category_code, e.category_code) AS category_code,
        CASE e.event_type
            WHEN 'purchase' THEN 5
            WHEN 'cart' THEN 3
            WHEN 'view' THEN 1
            WHEN 'remove_from_cart' THEN -1
            ELSE 0
        END AS weight
    WHERE category_id IS NOT NULL
    WITH category_id, category_code, sum(weight) AS category_affinity_score
    WHERE category_affinity_score > 0
    RETURN collect({
        category_id: category_id,
        category_code: category_code,
        category_affinity_score: category_affinity_score
    }) AS affinities
}
CALL {
    WITH target
    MATCH (target)-[e:INTERACTED_WITH]->(p:Product)
    WHERE e.event_type = 'purchase'
    RETURN collect(DISTINCT p.product_id) AS purchased_product_ids
}
WITH target_user_id, affinities, purchased_product_ids
UNWIND affinities AS affinity
CALL {
    WITH affinity
    MATCH (:User)-[e:INTERACTED_WITH]->(p:Product)
    WHERE coalesce(p.category_id, e.category_id) = affinity.category_id
    WITH
        p,
        affinity,
        sum(
            CASE e.event_type
                WHEN 'purchase' THEN 5
                WHEN 'cart' THEN 3
                WHEN 'view' THEN 1
                WHEN 'remove_from_cart' THEN -1
                ELSE 0
            END
        ) AS product_popularity_score
    WHERE product_popularity_score > 0
    RETURN collect({
        product_id: p.product_id,
        brand: p.brand,
        price: p.price,
        category_id: affinity.category_id,
        category_code: coalesce(p.category_code, affinity.category_code),
        category_affinity_score: affinity.category_affinity_score,
        product_popularity_score: product_popularity_score,
        recommendation_score: affinity.category_affinity_score * 100 + product_popularity_score
    }) AS candidate_products
}
UNWIND candidate_products AS recommendation
WITH target_user_id, purchased_product_ids, recommendation
WHERE NOT recommendation.product_id IN purchased_product_ids
RETURN
    target_user_id,
    recommendation.product_id AS product_id,
    recommendation.brand AS brand,
    recommendation.price AS price,
    recommendation.category_id AS category_id,
    recommendation.category_code AS category_code,
    recommendation.category_affinity_score AS category_affinity_score,
    recommendation.product_popularity_score AS product_popularity_score,
    recommendation.recommendation_score AS recommendation_score
ORDER BY category_affinity_score DESC,
         product_popularity_score DESC,
         product_id ASC
LIMIT 10;
