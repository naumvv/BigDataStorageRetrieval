
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, time as dtime, timezone
from typing import Any, Dict, Iterable, List

from pymongo import MongoClient, UpdateOne

from common import CLIENT_ID_PREFIX_DEFAULT, prepare_frames, records_from_frame


BATCH_SIZE = 10000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create and load the MongoDB solution schema.")
    parser.add_argument("--data-dir", required=True, help="Directory that contains the raw or cleaned CSV files.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=27017)
    parser.add_argument("--dbname", default="bigdata_assignment2")
    parser.add_argument("--drop", action="store_true", help="Drop the target database before loading.")
    parser.add_argument(
        "--client-id-prefix",
        default=CLIENT_ID_PREFIX_DEFAULT,
        help="Prefix used to derive user_id from client_id when user_device_id is available.",
    )
    return parser.parse_args()


def mongoize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, dtime.min, tzinfo=timezone.utc)
    return value


def mongo_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    converted = []
    for record in records:
        converted.append({key: mongoize(value) for key, value in record.items()})
    return converted


def insert_many_batched(collection, docs: List[Dict[str, Any]], label: str) -> None:
    if not docs:
        print(f"  {label}: 0 rows")
        return
    inserted = 0
    for offset in range(0, len(docs), BATCH_SIZE):
        batch = docs[offset : offset + BATCH_SIZE]
        collection.insert_many(batch, ordered=False)
        inserted += len(batch)
    print(f"  {label}: {inserted:,} rows")


def upsert_many_batched(collection, docs: List[Dict[str, Any]], key_field: str, label: str) -> None:
    if not docs:
        print(f"  {label}: 0 rows")
        return
    applied = 0
    for offset in range(0, len(docs), BATCH_SIZE):
        batch = docs[offset : offset + BATCH_SIZE]
        operations = [
            UpdateOne({key_field: doc[key_field]}, {"$set": doc}, upsert=True)
            for doc in batch
        ]
        collection.bulk_write(operations, ordered=False)
        applied += len(batch)
    print(f"  {label}: {applied:,} rows")


def build_friend_docs(friend_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = []
    for row in friend_records:
        user_id = row["user_id"]
        friend_id = row["friend_id"]
        docs.append({"user_id": user_id, "friend_id": friend_id})
        docs.append({"user_id": friend_id, "friend_id": user_id})
    return docs


def build_event_docs(event_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = []
    for row in event_records:
        doc = dict(row)
        docs.append(doc)
    return docs


def build_message_docs(message_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = []
    for row in message_records:
        doc = dict(row)
        docs.append(doc)
    return docs


def create_indexes(db) -> None:
    db.users.create_index("user_id", unique=True)
    db.clients.create_index("client_id", unique=True)
    db.clients.create_index("user_id")

    db.campaigns.create_index("campaign_key", unique=True)
    db.campaigns.create_index([("campaign_type", 1), ("channel", 1)])

    db.categories.create_index("category_id", unique=True)
    db.products.create_index("product_id", unique=True)
    db.products.create_index("category_id")
    db.products.create_index(
        [("category_code", "text"), ("brand", "text")],
        default_language="none",
        name="products_text",
    )

    db.events.create_index([("user_id", 1), ("event_time", 1)])
    db.events.create_index([("product_id", 1), ("event_type", 1), ("event_time", 1)])
    db.events.create_index([("category_id", 1), ("category_code", 1)])

    db.friends.create_index([("user_id", 1), ("friend_id", 1)], unique=True)

    db.messages.create_index([("campaign_key", 1), ("client_id", 1), ("sent_at", 1)])
    db.messages.create_index([("campaign_id", 1), ("message_type", 1)])
    db.messages.create_index([("user_id", 1), ("campaign_key", 1)])


def main() -> None:
    args = parse_args()
    t0 = time.time()

    frames = prepare_frames(args.data_dir, client_id_prefix=args.client_id_prefix)

    try:
        client = MongoClient(args.host, args.port)
        client.admin.command("ping")
    except Exception as exc:  # pragma: no cover - runtime connectivity
        print(f"Could not connect to MongoDB: {exc}")
        sys.exit(1)

    if args.drop:
        client.drop_database(args.dbname)

    db = client[args.dbname]

    print("Loading MongoDB collections...")

    user_docs = mongo_records(records_from_frame(frames["users"]))
    for doc in user_docs:
        doc["_id"] = doc["user_id"]
    upsert_many_batched(db.users, user_docs, "user_id", "users")

    client_docs = mongo_records(records_from_frame(frames["clients"]))
    for doc in client_docs:
        doc["_id"] = doc["client_id"]
    upsert_many_batched(db.clients, client_docs, "client_id", "clients")

    campaign_docs = mongo_records(records_from_frame(frames["campaigns"]))
    for doc in campaign_docs:
        doc["_id"] = doc["campaign_key"]
    upsert_many_batched(db.campaigns, campaign_docs, "campaign_key", "campaigns")

    category_docs = mongo_records(records_from_frame(frames["categories"]))
    for doc in category_docs:
        doc["_id"] = doc["category_id"]
    upsert_many_batched(db.categories, category_docs, "category_id", "categories")

    product_docs = mongo_records(records_from_frame(frames["products"]))
    for doc in product_docs:
        doc["_id"] = doc["product_id"]
    upsert_many_batched(db.products, product_docs, "product_id", "products")

    if args.drop:
        db.events.drop()
        db.friends.drop()
        db.messages.drop()

    event_docs = mongo_records(build_event_docs(records_from_frame(frames["events"])))
    insert_many_batched(db.events, event_docs, "events")

    friend_docs = mongo_records(build_friend_docs(records_from_frame(frames["friends"])))
    insert_many_batched(db.friends, friend_docs, "friends")

    message_docs = mongo_records(build_message_docs(records_from_frame(frames["messages"])))
    insert_many_batched(db.messages, message_docs, "messages")

    print("Creating indexes...")
    create_indexes(db)

    print("Collection counts:")
    for collection_name in [
        "users",
        "clients",
        "campaigns",
        "categories",
        "products",
        "events",
        "friends",
        "messages",
    ]:
        print(f"  {collection_name}: {db[collection_name].estimated_document_count():,}")

    print(f"MongoDB load completed in {time.time() - t0:.1f}s")
    client.close()


if __name__ == "__main__":
    main()
