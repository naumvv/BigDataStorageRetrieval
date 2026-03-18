
const db = db.getSiblingDB(
  (typeof process !== "undefined" && process.env && process.env.BIGDATA_DBNAME) ||
  "bigdata_assignment2"
);

function section(title, docs) {
  print("");
  print("=".repeat(80));
  print(title);
  print("=".repeat(80));
  printjson(docs.slice ? docs.slice(0, 20) : docs);
  if (Array.isArray(docs) && docs.length > 20)
    print(`... (${docs.length - 20} more rows omitted)`);
}


const directEffectiveness = db.messages.aggregate([
  {
    $lookup: {
      from:     "campaigns",
      let:      { cid: "$campaign_id", mtype: "$message_type" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$campaign_id",   "$$cid"]   },
                { $eq: ["$campaign_type", "$$mtype"] },
              ],
            },
          },
        },
      ],
      as: "campaign",
    },
  },
  { $unwind: "$campaign" },

  {
    $group: {
      _id: {
        campaign_id:      "$campaign_id",
        message_type:     "$message_type",
        campaign_channel: "$campaign.channel",
        topic:            "$campaign.topic",
        message_channel:  "$channel",
      },
      sent:        { $sum: 1 },
      opened:      { $sum: { $cond: [{ $eq: ["$engagement.is_opened",    true] }, 1, 0] } },
      clicked:     { $sum: { $cond: [{ $eq: ["$engagement.is_clicked",   true] }, 1, 0] } },
      purchased:   { $sum: { $cond: [{ $eq: ["$engagement.is_purchased", true] }, 1, 0] } },
      unsubscribed:{ $sum: { $cond: [{ $eq: ["$delivery.is_unsubscribed",true] }, 1, 0] } },
      hard_bounced:{ $sum: { $cond: [{ $eq: ["$delivery.is_hard_bounced",true] }, 1, 0] } },
      complained:  { $sum: { $cond: [{ $eq: ["$delivery.is_complained",  true] }, 1, 0] } },
    },
  },

  {
    $addFields: {
      open_rate:     { $cond: [{ $gt: ["$sent", 0] }, { $round: [{ $divide: ["$opened",   "$sent"] }, 4] }, null] },
      ctr:           { $cond: [{ $gt: ["$sent", 0] }, { $round: [{ $divide: ["$clicked",  "$sent"] }, 4] }, null] },
      purchase_rate: { $cond: [{ $gt: ["$sent", 0] }, { $round: [{ $divide: ["$purchased","$sent"] }, 4] }, null] },
    },
  },

  {
    $project: {
      _id: 0,
      campaign_id:      "$_id.campaign_id",
      message_type:     "$_id.message_type",
      campaign_channel: "$_id.campaign_channel",
      topic:            "$_id.topic",
      message_channel:  "$_id.message_channel",
      sent: 1, opened: 1, clicked: 1, purchased: 1,
      unsubscribed: 1, hard_bounced: 1, complained: 1,
      open_rate: 1, ctr: 1, purchase_rate: 1,
    },
  },

  { $sort:  { purchase_rate: -1, purchased: -1, sent: -1 } },
  { $limit: 50 },
]).toArray();

section("Q1 / Part 1 / Direct campaign effectiveness", directEffectiveness);

const WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

const purchaseIndex = new Map();

db.events.find(
  { event_type: "purchase", user_id: { $ne: null } },
  { user_id: 1, event_time: 1, _id: 0 }
).forEach(ev => {
  const uid = ev.user_id;
  const ts  = new Date(ev.event_time).getTime();
  if (!purchaseIndex.has(uid)) purchaseIndex.set(uid, []);
  purchaseIndex.get(uid).push(ts);
});

for (const [uid, ts] of purchaseIndex) ts.sort((a, b) => a - b);

function hadPurchaseInRange(userId, fromMs, toMs) {
  const list = purchaseIndex.get(userId);
  if (!list) return false;
  return list.some(t => t >= fromMs && t <= toMs);
}

const firstTouches = db.messages.aggregate([
  { $match: { user_id: { $ne: null }, sent_at: { $ne: null } } },
  {
    $group: {
      _id: {
        campaign_id:  "$campaign_id",
        message_type: "$message_type",
        user_id:      "$user_id",
      },
      first_touch: { $min: "$sent_at" },
    },
  },
]).toArray();

const liftMap = new Map();

for (const row of firstTouches) {
  const { campaign_id, message_type, user_id } = row._id;
  const touchMs = new Date(row.first_touch).getTime();
  const key     = `${message_type}::${campaign_id}`;

  const before = hadPurchaseInRange(user_id, touchMs - WINDOW_MS, touchMs - 1);
  const after  = hadPurchaseInRange(user_id, touchMs + 1,         touchMs + WINDOW_MS);

  if (!liftMap.has(key)) {
    liftMap.set(key, {
      campaign_id, message_type,
      recipients: 0,
      purchase_before: 0,
      purchase_after:  0,
    });
  }
  const entry = liftMap.get(key);
  entry.recipients++;
  if (before) entry.purchase_before++;
  if (after)  entry.purchase_after++;
}

const campaignIds = [...new Set(firstTouches.map(r => r._id.campaign_id))];

const campaignMeta = new Map();
db.campaigns.find({ campaign_id: { $in: campaignIds } })
  .forEach(c => campaignMeta.set(`${c.campaign_type}::${c.campaign_id}`, c));

const purchaseLift = [...liftMap.values()]
  .map(e => {
    const meta = campaignMeta.get(`${e.message_type}::${e.campaign_id}`) || {};
    const pre  = e.recipients > 0 ? e.purchase_before / e.recipients : 0;
    const post = e.recipients > 0 ? e.purchase_after  / e.recipients : 0;
    return {
      campaign_id:                    e.campaign_id,
      message_type:                   e.message_type,
      campaign_channel:               meta.channel  || null,
      topic:                          meta.topic    || null,
      recipients:                     e.recipients,
      recipients_with_purchase_before:e.purchase_before,
      recipients_with_purchase_after: e.purchase_after,
      purchase_before_rate:           Math.round(pre  * 1e4) / 1e4,
      purchase_after_rate:            Math.round(post * 1e4) / 1e4,
      absolute_lift:                  Math.round((post - pre) * 1e4) / 1e4,
    };
  })
  .sort((a, b) => b.absolute_lift - a.absolute_lift || b.recipients - a.recipients);

section("Q1 / Part 2 / 7-day purchase lift", purchaseLift);

const convertedSet = new Set(
  db.messages.aggregate([
    { $match: { "engagement.is_purchased": true, user_id: { $ne: null } } },
    { $group: { _id: "$user_id" } },
  ]).toArray().map(r => r._id)
);

const userCategoryMap = new Map();
db.events.aggregate([
  { $match: { user_id: { $ne: null }, "product.category_code": { $ne: null } } },
  {
    $group: {
      _id:        "$user_id",
      categories: { $addToSet: "$product.category_code" },
    },
  },
]).forEach(row => userCategoryMap.set(row._id, new Set(row.categories)));


const candidateScores = new Map();

db.clients.find(
  { user_id: { $ne: null }, friends: { $exists: true, $not: { $size: 0 } } },
  { user_id: 1, friends: 1, _id: 0 }
).forEach(client => {
  const candidateId   = client.user_id;
  if (convertedSet.has(candidateId)) return;

  const candidateCats = userCategoryMap.get(candidateId) || new Set();
  let convertedFriendsCount = 0;
  let overlapScore          = 0;

  for (const friendId of (client.friends || [])) {
    if (!convertedSet.has(friendId)) continue;
    convertedFriendsCount++;
    const friendCats = userCategoryMap.get(friendId) || new Set();
    for (const cat of friendCats) {
      if (candidateCats.has(cat)) overlapScore++;
    }
  }

  if (convertedFriendsCount === 0) return;

  if (!candidateScores.has(candidateId)) {
    candidateScores.set(candidateId, { convertedFriendsCount: 0, overlapScore: 0 });
  }
  const s = candidateScores.get(candidateId);
  s.convertedFriendsCount += convertedFriendsCount;
  s.overlapScore          += overlapScore;
});

const socialTargets = [...candidateScores.entries()]
  .map(([uid, s]) => ({
    candidate_user_id:       uid,
    converted_friends_count: s.convertedFriendsCount,
    category_overlap_score:  s.overlapScore,
    recommendation_score:    s.convertedFriendsCount * 10 + s.overlapScore,
  }))
  .sort((a, b) => b.recommendation_score - a.recommendation_score)
  .slice(0, 50);

section("Q1 / Part 3 / Social targeting candidates", socialTargets);
