
-- q1.sql
-- Goal:
-- 1) Measure direct campaign effectiveness using message outcomes.
-- 2) Estimate incremental lift with a 7-day before/after window around first campaign exposure.
-- 3) Suggest social-network targets for future campaigns by expanding from converted recipients.

-- ------------------------------------------------------------------
-- Part 1. Direct campaign effectiveness
-- ------------------------------------------------------------------
WITH campaign_kpis AS (
    SELECT
        m.campaign_id,
        m.message_type,
        c.channel AS campaign_channel,
        c.topic,
        m.channel AS message_channel,
        COUNT(*) AS sent_count,
        SUM(CASE WHEN COALESCE(m.is_opened, FALSE) THEN 1 ELSE 0 END) AS opened_count,
        SUM(CASE WHEN COALESCE(m.is_clicked, FALSE) THEN 1 ELSE 0 END) AS clicked_count,
        SUM(CASE WHEN COALESCE(m.is_purchased, FALSE) THEN 1 ELSE 0 END) AS purchased_count,
        SUM(CASE WHEN COALESCE(m.is_unsubscribed, FALSE) THEN 1 ELSE 0 END) AS unsubscribed_count,
        SUM(CASE WHEN COALESCE(m.is_hard_bounced, FALSE) THEN 1 ELSE 0 END) AS hard_bounced_count,
        SUM(CASE WHEN COALESCE(m.is_complained, FALSE) THEN 1 ELSE 0 END) AS complained_count
    FROM messages AS m
    JOIN campaigns AS c
      ON c.campaign_id = m.campaign_id
     AND c.campaign_type = m.message_type
    GROUP BY
        m.campaign_id,
        m.message_type,
        c.channel,
        c.topic,
        m.channel
)
SELECT
    campaign_id,
    message_type,
    campaign_channel,
    topic,
    message_channel,
    sent_count,
    opened_count,
    clicked_count,
    purchased_count,
    unsubscribed_count,
    hard_bounced_count,
    complained_count,
    ROUND(opened_count::numeric / NULLIF(sent_count, 0), 4) AS open_rate,
    ROUND(clicked_count::numeric / NULLIF(sent_count, 0), 4) AS click_rate,
    ROUND(purchased_count::numeric / NULLIF(sent_count, 0), 4) AS purchase_rate,
    ROUND(purchased_count::numeric / NULLIF(clicked_count, 0), 4) AS click_to_purchase_rate
FROM campaign_kpis
ORDER BY purchase_rate DESC, purchased_count DESC, sent_count DESC
LIMIT 50;

-- ------------------------------------------------------------------
-- Part 2. Purchase lift: 7 days before vs. 7 days after first exposure
-- ------------------------------------------------------------------
WITH recipient_first_touch AS (
    SELECT
        m.campaign_id,
        m.message_type,
        COALESCE(m.user_id, cl.user_id) AS user_id,
        MIN(m.sent_at) AS first_touch_at
    FROM messages AS m
    JOIN clients AS cl
      ON cl.client_id = m.client_id
    WHERE COALESCE(m.user_id, cl.user_id) IS NOT NULL
      AND m.sent_at IS NOT NULL
    GROUP BY
        m.campaign_id,
        m.message_type,
        COALESCE(m.user_id, cl.user_id)
),
recipient_lift AS (
    SELECT
        rft.campaign_id,
        rft.message_type,
        rft.user_id,
        EXISTS (
            SELECT 1
            FROM events AS e
            WHERE e.user_id = rft.user_id
              AND e.event_type = 'purchase'
              AND e.event_time >= rft.first_touch_at - INTERVAL '7 days'
              AND e.event_time < rft.first_touch_at
        ) AS had_purchase_before,
        EXISTS (
            SELECT 1
            FROM events AS e
            WHERE e.user_id = rft.user_id
              AND e.event_type = 'purchase'
              AND e.event_time > rft.first_touch_at
              AND e.event_time <= rft.first_touch_at + INTERVAL '7 days'
        ) AS had_purchase_after
    FROM recipient_first_touch AS rft
)
SELECT
    rl.campaign_id,
    rl.message_type,
    c.channel AS campaign_channel,
    c.topic,
    COUNT(*) AS recipients,
    SUM((rl.had_purchase_before)::int) AS recipients_with_purchase_before,
    SUM((rl.had_purchase_after)::int) AS recipients_with_purchase_after,
    ROUND(AVG((rl.had_purchase_before)::int), 4) AS purchase_before_rate,
    ROUND(AVG((rl.had_purchase_after)::int), 4) AS purchase_after_rate,
    ROUND(AVG((rl.had_purchase_after)::int) - AVG((rl.had_purchase_before)::int), 4) AS absolute_lift
FROM recipient_lift AS rl
JOIN campaigns AS c
  ON c.campaign_id = rl.campaign_id
 AND c.campaign_type = rl.message_type
GROUP BY
    rl.campaign_id,
    rl.message_type,
    c.channel,
    c.topic
ORDER BY absolute_lift DESC, recipients DESC
LIMIT 50;

-- ------------------------------------------------------------------
-- Part 3. Social-network targeting for the next campaign
-- Converted recipients' friends are ranked by:
--   a) number of converted friends
--   b) overlap between the candidate's browsed/purchased categories
--      and the converted friends' purchased categories
-- ------------------------------------------------------------------
WITH converted_recipients AS (
    SELECT DISTINCT
        m.campaign_id,
        m.message_type,
        COALESCE(m.user_id, cl.user_id) AS converter_user_id
    FROM messages AS m
    JOIN clients AS cl
      ON cl.client_id = m.client_id
    WHERE COALESCE(m.user_id, cl.user_id) IS NOT NULL
      AND COALESCE(m.is_purchased, FALSE)
),
friend_candidates AS (
    SELECT
        cr.campaign_id,
        cr.message_type,
        cr.converter_user_id,
        CASE
            WHEN f.user_id = cr.converter_user_id THEN f.friend_id
            ELSE f.user_id
        END AS candidate_user_id
    FROM converted_recipients AS cr
    JOIN friends AS f
      ON f.user_id = cr.converter_user_id
      OR f.friend_id = cr.converter_user_id
),
filtered_candidates AS (
    SELECT fc.*
    FROM friend_candidates AS fc
    LEFT JOIN converted_recipients AS already_converted
      ON already_converted.campaign_id = fc.campaign_id
     AND already_converted.message_type = fc.message_type
     AND already_converted.converter_user_id = fc.candidate_user_id
    WHERE already_converted.converter_user_id IS NULL
),
converter_purchase_categories AS (
    SELECT DISTINCT
        e.user_id AS converter_user_id,
        COALESCE(p.category_code, e.category_code) AS category_code
    FROM events AS e
    LEFT JOIN products AS p
      ON p.product_id = e.product_id
    WHERE e.event_type = 'purchase'
      AND COALESCE(p.category_code, e.category_code) IS NOT NULL
),
candidate_interest_categories AS (
    SELECT DISTINCT
        e.user_id AS candidate_user_id,
        COALESCE(p.category_code, e.category_code) AS category_code
    FROM events AS e
    LEFT JOIN products AS p
      ON p.product_id = e.product_id
    WHERE e.event_type IN ('view', 'cart', 'purchase')
      AND COALESCE(p.category_code, e.category_code) IS NOT NULL
),
pairwise_overlap AS (
    SELECT
        fc.campaign_id,
        fc.message_type,
        fc.candidate_user_id,
        fc.converter_user_id,
        COUNT(DISTINCT cic.category_code) AS overlap_categories
    FROM filtered_candidates AS fc
    LEFT JOIN converter_purchase_categories AS cpc
      ON cpc.converter_user_id = fc.converter_user_id
    LEFT JOIN candidate_interest_categories AS cic
      ON cic.candidate_user_id = fc.candidate_user_id
     AND cic.category_code = cpc.category_code
    GROUP BY
        fc.campaign_id,
        fc.message_type,
        fc.candidate_user_id,
        fc.converter_user_id
),
scored_candidates AS (
    SELECT
        campaign_id,
        message_type,
        candidate_user_id,
        COUNT(DISTINCT converter_user_id) AS converted_friends_count,
        COALESCE(SUM(overlap_categories), 0) AS category_overlap_score
    FROM pairwise_overlap
    GROUP BY
        campaign_id,
        message_type,
        candidate_user_id
)
SELECT
    sc.campaign_id,
    sc.message_type,
    c.channel AS campaign_channel,
    c.topic,
    sc.candidate_user_id,
    sc.converted_friends_count,
    sc.category_overlap_score,
    sc.converted_friends_count * 10 + sc.category_overlap_score AS recommendation_score
FROM scored_candidates AS sc
JOIN campaigns AS c
  ON c.campaign_id = sc.campaign_id
 AND c.campaign_type = sc.message_type
ORDER BY
    recommendation_score DESC,
    converted_friends_count DESC,
    category_overlap_score DESC,
    candidate_user_id
LIMIT 100;
