
// q1.cypher
// Part 1: Direct campaign effectiveness.
MATCH (cl:Client)-[m:RECEIVED_MESSAGE]->(c:Campaign)
WITH
    m.campaign_id AS campaign_id,
    m.message_type AS message_type,
    c.channel AS campaign_channel,
    c.topic AS topic,
    m.channel AS message_channel,
    count(m) AS sent_count,
    sum(CASE WHEN coalesce(m.is_opened, false) THEN 1 ELSE 0 END) AS opened_count,
    sum(CASE WHEN coalesce(m.is_clicked, false) THEN 1 ELSE 0 END) AS clicked_count,
    sum(CASE WHEN coalesce(m.is_purchased, false) THEN 1 ELSE 0 END) AS purchased_count,
    sum(CASE WHEN coalesce(m.is_unsubscribed, false) THEN 1 ELSE 0 END) AS unsubscribed_count,
    sum(CASE WHEN coalesce(m.is_hard_bounced, false) THEN 1 ELSE 0 END) AS hard_bounced_count,
    sum(CASE WHEN coalesce(m.is_complained, false) THEN 1 ELSE 0 END) AS complained_count
RETURN
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
    CASE WHEN sent_count > 0
         THEN round(10000.0 * toFloat(opened_count) / sent_count) / 10000.0
         ELSE null END AS open_rate,
    CASE WHEN sent_count > 0
         THEN round(10000.0 * toFloat(clicked_count) / sent_count) / 10000.0
         ELSE null END AS click_rate,
    CASE WHEN sent_count > 0
         THEN round(10000.0 * toFloat(purchased_count) / sent_count) / 10000.0
         ELSE null END AS purchase_rate,
    CASE WHEN clicked_count > 0
         THEN round(10000.0 * toFloat(purchased_count) / clicked_count) / 10000.0
         ELSE null END AS click_to_purchase_rate
ORDER BY purchase_rate DESC, purchased_count DESC, sent_count DESC, campaign_id ASC
LIMIT 50;

// Part 2: 7-day purchase lift around first exposure.
MATCH (cl:Client)-[:BELONGS_TO]->(u:User)
MATCH (cl)-[m:RECEIVED_MESSAGE]->(c:Campaign)
WHERE m.sent_at IS NOT NULL
WITH c, u, min(m.sent_at) AS first_touch_at
CALL {
    WITH u, first_touch_at
    MATCH (u)-[e:INTERACTED_WITH]->(:Product)
    WHERE e.event_type = 'purchase'
      AND e.event_time >= first_touch_at - duration({days: 7})
      AND e.event_time < first_touch_at
    RETURN count(e) > 0 AS had_purchase_before
}
CALL {
    WITH u, first_touch_at
    MATCH (u)-[e:INTERACTED_WITH]->(:Product)
    WHERE e.event_type = 'purchase'
      AND e.event_time > first_touch_at
      AND e.event_time <= first_touch_at + duration({days: 7})
    RETURN count(e) > 0 AS had_purchase_after
}
WITH
    c,
    count(*) AS recipients,
    sum(CASE WHEN had_purchase_before THEN 1 ELSE 0 END) AS recipients_with_purchase_before,
    sum(CASE WHEN had_purchase_after THEN 1 ELSE 0 END) AS recipients_with_purchase_after
RETURN
    c.campaign_id AS campaign_id,
    c.campaign_type AS message_type,
    c.channel AS campaign_channel,
    c.topic AS topic,
    recipients,
    recipients_with_purchase_before,
    recipients_with_purchase_after,
    CASE WHEN recipients > 0
         THEN round(10000.0 * toFloat(recipients_with_purchase_before) / recipients) / 10000.0
         ELSE null END AS purchase_before_rate,
    CASE WHEN recipients > 0
         THEN round(10000.0 * toFloat(recipients_with_purchase_after) / recipients) / 10000.0
         ELSE null END AS purchase_after_rate,
    CASE WHEN recipients > 0
         THEN round(
             10000.0 * (
                 toFloat(recipients_with_purchase_after) / recipients -
                 toFloat(recipients_with_purchase_before) / recipients
             )
         ) / 10000.0
         ELSE null END AS absolute_lift
ORDER BY absolute_lift DESC, recipients DESC, campaign_id ASC
LIMIT 50;

// Part 3: Social-network targeting candidates.
// Rank friends of converted recipients by:
//   a) number of converted friends
//   b) overlap between candidate interests and converted friends' purchase categories.
MATCH (converterClient:Client)-[:BELONGS_TO]->(converter:User)
MATCH (converterClient)-[m:RECEIVED_MESSAGE]->(c:Campaign)
WHERE coalesce(m.is_purchased, false) = true
WITH DISTINCT c, converter
MATCH (converter)-[:FRIENDS_WITH]->(candidate:User)
WHERE candidate.user_id <> converter.user_id
  AND NOT EXISTS {
      MATCH (candidateClient:Client)-[:BELONGS_TO]->(candidate)
      MATCH (candidateClient)-[candidateMessage:RECEIVED_MESSAGE]->(c)
      WHERE coalesce(candidateMessage.is_purchased, false) = true
  }
CALL {
    WITH converter
    MATCH (converter)-[e:INTERACTED_WITH]->(p:Product)
    WHERE e.event_type = 'purchase'
      AND coalesce(p.category_code, e.category_code) IS NOT NULL
    RETURN collect(DISTINCT coalesce(p.category_code, e.category_code)) AS converter_categories
}
CALL {
    WITH candidate
    MATCH (candidate)-[e:INTERACTED_WITH]->(p:Product)
    WHERE e.event_type IN ['view', 'cart', 'purchase']
      AND coalesce(p.category_code, e.category_code) IS NOT NULL
    RETURN collect(DISTINCT coalesce(p.category_code, e.category_code)) AS candidate_categories
}
WITH
    c,
    candidate.user_id AS candidate_user_id,
    converter.user_id AS converter_user_id,
    size([category IN converter_categories WHERE category IN candidate_categories]) AS overlap_categories
WITH
    c,
    candidate_user_id,
    count(DISTINCT converter_user_id) AS converted_friends_count,
    sum(overlap_categories) AS category_overlap_score
RETURN
    c.campaign_id AS campaign_id,
    c.campaign_type AS message_type,
    c.channel AS campaign_channel,
    c.topic AS topic,
    candidate_user_id,
    converted_friends_count,
    category_overlap_score,
    converted_friends_count * 10 + category_overlap_score AS recommendation_score
ORDER BY recommendation_score DESC,
         converted_friends_count DESC,
         category_overlap_score DESC,
         candidate_user_id ASC
LIMIT 100;
