
from __future__ import annotations

import argparse
import sys
import time
from typing import Any, Dict, List

from neo4j import GraphDatabase

from common import CLIENT_ID_PREFIX_DEFAULT, prepare_frames, records_from_frame


BATCH_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create and load the Neo4j solution graph.")
    parser.add_argument("--data-dir", required=True, help="Directory that contains the raw or cleaned CSV files.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--username", default="neo4j")
    parser.add_argument("--password", default="password")
    parser.add_argument("--drop", action="store_true", help="Delete all nodes and relationships before loading.")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument(
        "--client-id-prefix",
        default=CLIENT_ID_PREFIX_DEFAULT,
        help="Prefix used to derive user_id from client_id when user_device_id is available.",
    )
    return parser.parse_args()


def run_batches(session, query: str, rows: List[Dict[str, Any]], label: str, batch_size: int) -> None:
    if not rows:
        print(f"  {label}: 0 rows")
        return

    total = len(rows)
    inserted = 0
    t0 = time.time()
    for offset in range(0, total, batch_size):
        batch = rows[offset : offset + batch_size]

        def write_chunk(tx, b=batch):
            tx.run(query, rows=b).consume()

        session.execute_write(write_chunk)
        inserted += len(batch)
        pct = inserted / total * 100
        elapsed = time.time() - t0
        print(f"  {label}: {inserted:,}/{total:,}  ({pct:.0f}%)  {elapsed:.1f}s", end="\r", flush=True)
    print(f"  {label}: {inserted:,} rows  ({time.time() - t0:.1f}s)            ")


def create_constraints_and_indexes(session) -> None:
    statements = [
        "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
        "CREATE CONSTRAINT client_id_unique IF NOT EXISTS FOR (c:Client) REQUIRE c.client_id IS UNIQUE",
        "CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE",
        "CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE",
        "CREATE CONSTRAINT campaign_key_unique IF NOT EXISTS FOR (c:Campaign) REQUIRE c.campaign_key IS UNIQUE",
        "CREATE INDEX product_category_id IF NOT EXISTS FOR (p:Product) ON (p.category_id)",
        "CREATE INDEX campaign_type_channel IF NOT EXISTS FOR (c:Campaign) ON (c.campaign_type, c.channel)",
        "CREATE INDEX interacted_event_type IF NOT EXISTS FOR ()-[r:INTERACTED_WITH]-() ON (r.event_type)",
        "CREATE INDEX interacted_event_time IF NOT EXISTS FOR ()-[r:INTERACTED_WITH]-() ON (r.event_time)",
        "CREATE INDEX received_sent_at IF NOT EXISTS FOR ()-[r:RECEIVED_MESSAGE]-() ON (r.sent_at)",
        "CREATE INDEX received_is_purchased IF NOT EXISTS FOR ()-[r:RECEIVED_MESSAGE]-() ON (r.is_purchased)",
        "CREATE FULLTEXT INDEX productFulltext IF NOT EXISTS FOR (p:Product) ON EACH [p.category_code, p.brand]",
    ]
    for statement in statements:
        session.run(statement).consume()


def clear_graph(session) -> None:
    # CALL { } IN TRANSACTIONS splits the delete across many small sub-transactions
    # so it stays within Neo4j's per-transaction memory limit.
    session.run(
        "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 5000 ROWS"
    ).consume()


def main() -> None:
    args = parse_args()
    t0 = time.time()

    frames = prepare_frames(args.data_dir, client_id_prefix=args.client_id_prefix)

    try:
        driver = GraphDatabase.driver(args.uri, auth=(args.username, args.password))
        driver.verify_connectivity()
    except Exception as exc:  # pragma: no cover - runtime connectivity
        print(f"Could not connect to Neo4j: {exc}")
        sys.exit(1)

    try:
        with driver.session() as session:
            if args.drop:
                print("Deleting existing graph...")
                clear_graph(session)

            print("Creating constraints and indexes...")
            create_constraints_and_indexes(session)

            users = records_from_frame(frames["users"])
            run_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (u:User {user_id: row.user_id})
                """,
                users,
                "users",
                args.batch_size,
            )

            clients = records_from_frame(frames["clients"])
            run_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (c:Client {client_id: row.client_id})
                SET c.user_device_id = row.user_device_id,
                    c.first_purchase_date = row.first_purchase_date
                FOREACH (_ IN CASE WHEN row.user_id IS NULL THEN [] ELSE [1] END |
                    MERGE (u:User {user_id: row.user_id})
                    MERGE (c)-[:BELONGS_TO]->(u)
                )
                """,
                clients,
                "clients",
                args.batch_size,
            )

            categories = records_from_frame(frames["categories"])
            run_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (c:Category {category_id: row.category_id})
                SET c.category_code = row.category_code
                """,
                categories,
                "categories",
                args.batch_size,
            )

            products = records_from_frame(frames["products"])
            run_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (p:Product {product_id: row.product_id})
                SET p.category_id = row.category_id,
                    p.category_code = row.category_code,
                    p.brand = row.brand,
                    p.price = row.price
                FOREACH (_ IN CASE WHEN row.category_id IS NULL THEN [] ELSE [1] END |
                    MERGE (c:Category {category_id: row.category_id})
                    ON CREATE SET c.category_code = row.category_code
                    SET c.category_code = coalesce(c.category_code, row.category_code)
                    MERGE (p)-[:IN_CATEGORY]->(c)
                )
                """,
                products,
                "products",
                args.batch_size,
            )

            campaigns = records_from_frame(frames["campaigns"])
            run_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (c:Campaign {campaign_key: row.campaign_key})
                SET c.campaign_id = row.campaign_id,
                    c.campaign_type = row.campaign_type,
                    c.channel = row.channel,
                    c.topic = row.topic,
                    c.started_at = row.started_at,
                    c.finished_at = row.finished_at,
                    c.total_count = row.total_count,
                    c.ab_test = row.ab_test,
                    c.warmup_mode = row.warmup_mode,
                    c.hour_limit = row.hour_limit,
                    c.subject_length = row.subject_length,
                    c.subject_with_personalization = row.subject_with_personalization,
                    c.subject_with_deadline = row.subject_with_deadline,
                    c.subject_with_emoji = row.subject_with_emoji,
                    c.subject_with_bonuses = row.subject_with_bonuses,
                    c.subject_with_discount = row.subject_with_discount,
                    c.subject_with_saleout = row.subject_with_saleout,
                    c.is_test = row.is_test,
                    c.position = row.position
                """,
                campaigns,
                "campaigns",
                args.batch_size,
            )

            events = records_from_frame(frames["events"])
            run_batches(
                session,
                """
                UNWIND $rows AS row
                MATCH (u:User {user_id: row.user_id})
                MATCH (p:Product {product_id: row.product_id})
                CREATE (u)-[:INTERACTED_WITH {
                    event_time: row.event_time,
                    event_type: row.event_type,
                    user_session: row.user_session,
                    category_id: row.category_id,
                    category_code: row.category_code,
                    brand: row.brand,
                    price: row.price
                }]->(p)
                """,
                events,
                "events",
                args.batch_size,
            )

            friends = records_from_frame(frames["friends"])
            run_batches(
                session,
                """
                UNWIND $rows AS row
                MATCH (u:User {user_id: row.user_id})
                MATCH (f:User {user_id: row.friend_id})
                MERGE (u)-[:FRIENDS_WITH]->(f)
                MERGE (f)-[:FRIENDS_WITH]->(u)
                """,
                friends,
                "friends",
                args.batch_size,
            )

            messages = records_from_frame(frames["messages"])
            run_batches(
                session,
                """
                UNWIND $rows AS row
                MATCH (cl:Client {client_id: row.client_id})
                MATCH (cp:Campaign {campaign_key: row.campaign_key})
                CREATE (cl)-[:RECEIVED_MESSAGE {
                    raw_message_id: row.raw_message_id,
                    campaign_id: row.campaign_id,
                    message_type: row.message_type,
                    campaign_key: row.campaign_key,
                    channel: row.channel,
                    user_id: row.user_id,
                    user_device_id: row.user_device_id,
                    email_provider: row.email_provider,
                    platform: row.platform,
                    stream: row.stream,
                    message_date: row.message_date,
                    sent_at: row.sent_at,
                    is_opened: row.is_opened,
                    opened_first_time_at: row.opened_first_time_at,
                    opened_last_time_at: row.opened_last_time_at,
                    is_clicked: row.is_clicked,
                    clicked_first_time_at: row.clicked_first_time_at,
                    clicked_last_time_at: row.clicked_last_time_at,
                    is_unsubscribed: row.is_unsubscribed,
                    unsubscribed_at: row.unsubscribed_at,
                    is_hard_bounced: row.is_hard_bounced,
                    hard_bounced_at: row.hard_bounced_at,
                    is_soft_bounced: row.is_soft_bounced,
                    soft_bounced_at: row.soft_bounced_at,
                    is_complained: row.is_complained,
                    complained_at: row.complained_at,
                    is_blocked: row.is_blocked,
                    blocked_at: row.blocked_at,
                    is_purchased: row.is_purchased,
                    purchased_at: row.purchased_at
                }]->(cp)
                """,
                messages,
                "messages",
                args.batch_size,
            )

            result = session.run(
                """
                MATCH (u:User) WITH count(u) AS users
                MATCH (c:Client) WITH users, count(c) AS clients
                MATCH (cp:Campaign) WITH users, clients, count(cp) AS campaigns
                MATCH (p:Product) WITH users, clients, campaigns, count(p) AS products
                MATCH ()-[e:INTERACTED_WITH]->() WITH users, clients, campaigns, products, count(e) AS events
                MATCH ()-[m:RECEIVED_MESSAGE]->() WITH users, clients, campaigns, products, events, count(m) AS messages
                MATCH ()-[f:FRIENDS_WITH]->() WITH users, clients, campaigns, products, events, messages, count(f) AS friend_links
                RETURN users, clients, campaigns, products, events, messages, friend_links
                """
            ).single()
            print("Graph counts:")
            for key, value in result.items():
                print(f"  {key}: {value:,}")

        print(f"Neo4j load completed in {time.time() - t0:.1f}s")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
