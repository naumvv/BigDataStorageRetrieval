const DB_NAME =
  (typeof process !== "undefined" && process.env && process.env.BIGDATA_DBNAME) ||
  "bigdata_assignment2";
const database = db.getSiblingDB(DB_NAME);

function printSection(title, docs) {
  print("");
  print("=".repeat(80));
  print(title);
  print("=".repeat(80));
  printjson(docs);
}

function eventWeightExpression() {
  return {
    $switch: {
      branches: [
        { case: { $eq: ["$event_type", "purchase"] }, then: 5 },
        { case: { $eq: ["$event_type", "cart"] }, then: 3 },
        { case: { $eq: ["$event_type", "view"] }, then: 1 },
        { case: { $eq: ["$event_type", "remove_from_cart"] }, then: -1 },
      ],
      default: 0,
    },
  };
}

function getTargetUserId() {
  const rows = database.events
    .aggregate([
      {
        $match: {
          user_id: { $ne: null },
        },
      },
      {
        $group: {
          _id: "$user_id",
          event_count: { $sum: 1 },
        },
      },
      {
        $sort: {
          event_count: -1,
          _id: 1,
        },
      },
      { $limit: 1 },
    ])
    .toArray();

  return rows.length ? rows[0]._id : null;
}

function buildRecommendations(limitCount) {
  const targetUserId = getTargetUserId();
  if (targetUserId === null) {
    return { targetUserId: null, recommendations: [] };
  }

  const userCategoryAffinity = database.events
    .aggregate([
      {
        $match: {
          user_id: targetUserId,
        },
      },
      {
        $lookup: {
          from: "products",
          localField: "product_id",
          foreignField: "product_id",
          as: "product",
        },
      },
      {
        $addFields: {
          resolved_category_id: {
            $ifNull: ["$category_id", { $arrayElemAt: ["$product.category_id", 0] }],
          },
          resolved_category_code: {
            $ifNull: ["$category_code", { $arrayElemAt: ["$product.category_code", 0] }],
          },
          weight: eventWeightExpression(),
        },
      },
      {
        $match: {
          resolved_category_id: { $ne: null },
        },
      },
      {
        $group: {
          _id: {
            category_id: "$resolved_category_id",
            category_code: "$resolved_category_code",
          },
          category_affinity_score: { $sum: "$weight" },
        },
      },
      {
        $match: {
          category_affinity_score: { $gt: 0 },
        },
      },
      {
        $project: {
          _id: 0,
          category_id: "$_id.category_id",
          category_code: "$_id.category_code",
          category_affinity_score: 1,
        },
      },
    ])
    .toArray();

  const purchasedProductIds = new Set(
    database.events.distinct("product_id", {
      user_id: targetUserId,
      event_type: "purchase",
    })
  );

  const productPopularity = database.events
    .aggregate([
      {
        $lookup: {
          from: "products",
          localField: "product_id",
          foreignField: "product_id",
          as: "product",
        },
      },
      {
        $addFields: {
          resolved_category_id: {
            $ifNull: ["$category_id", { $arrayElemAt: ["$product.category_id", 0] }],
          },
          resolved_category_code: {
            $ifNull: ["$category_code", { $arrayElemAt: ["$product.category_code", 0] }],
          },
          resolved_brand: {
            $ifNull: ["$brand", { $arrayElemAt: ["$product.brand", 0] }],
          },
          resolved_price: {
            $ifNull: ["$price", { $arrayElemAt: ["$product.price", 0] }],
          },
          weight: eventWeightExpression(),
        },
      },
      {
        $match: {
          resolved_category_id: { $ne: null },
        },
      },
      {
        $group: {
          _id: "$product_id",
          category_id: { $first: "$resolved_category_id" },
          category_code: { $first: "$resolved_category_code" },
          brand: { $max: "$resolved_brand" },
          price: { $max: "$resolved_price" },
          product_popularity_score: { $sum: "$weight" },
        },
      },
      {
        $match: {
          product_popularity_score: { $gt: 0 },
        },
      },
      {
        $project: {
          _id: 0,
          product_id: "$_id",
          category_id: 1,
          category_code: 1,
          brand: 1,
          price: 1,
          product_popularity_score: 1,
        },
      },
    ])
    .toArray();

  const productsByCategory = new Map();
  for (const product of productPopularity) {
    if (!productsByCategory.has(product.category_id)) {
      productsByCategory.set(product.category_id, []);
    }
    productsByCategory.get(product.category_id).push(product);
  }

  const recommendations = [];
  for (const affinity of userCategoryAffinity) {
    const candidates = productsByCategory.get(affinity.category_id) || [];
    for (const product of candidates) {
      if (purchasedProductIds.has(product.product_id)) {
        continue;
      }
      recommendations.push({
        target_user_id: targetUserId,
        product_id: product.product_id,
        brand: product.brand,
        price: product.price,
        category_id: product.category_id,
        category_code: product.category_code,
        category_affinity_score: affinity.category_affinity_score,
        product_popularity_score: product.product_popularity_score,
        recommendation_score:
          affinity.category_affinity_score * 100 + product.product_popularity_score,
      });
    }
  }

  recommendations.sort((a, b) => {
    if (b.category_affinity_score !== a.category_affinity_score) {
      return b.category_affinity_score - a.category_affinity_score;
    }
    if (b.product_popularity_score !== a.product_popularity_score) {
      return b.product_popularity_score - a.product_popularity_score;
    }
    return a.product_id - b.product_id;
  });

  return {
    targetUserId,
    recommendations: recommendations.slice(0, limitCount),
  };
}

const result = buildRecommendations(10);
printSection("Q2 / Top personalized recommendations", result.recommendations);
