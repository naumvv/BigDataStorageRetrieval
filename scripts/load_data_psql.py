
from __future__ import annotations

import argparse
import io
import sys
import time
from typing import Iterable, List

import pandas as pd
import psycopg2
from psycopg2 import sql

from common import CLIENT_ID_PREFIX_DEFAULT, prepare_frames


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create and load the PostgreSQL solution schema.")
    parser.add_argument("--data-dir", required=True, help="Directory that contains the raw or cleaned CSV files.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="bigdata_assignment2")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="postgres")
    parser.add_argument("--maintenance-db", default="postgres", help="Database used to create the target database if needed.")
    parser.add_argument("--drop", action="store_true", help="Drop all assignment tables before loading.")
    parser.add_argument(
        "--client-id-prefix",
        default=CLIENT_ID_PREFIX_DEFAULT,
        help="Prefix used to derive user_id from client_id when user_device_id is available.",
    )
    return parser.parse_args()


def connect(args: argparse.Namespace, dbname: str):
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=dbname,
        user=args.user,
        password=args.password,
    )


def ensure_database(args: argparse.Namespace) -> None:
    conn = connect(args, args.maintenance_db)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (args.dbname,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(args.dbname)))
                print(f"Created database: {args.dbname}")
    finally:
        conn.close()


def drop_tables(cur) -> None:
    cur.execute(
        """
        DROP TABLE IF EXISTS messages CASCADE;
        DROP TABLE IF EXISTS friends CASCADE;
        DROP TABLE IF EXISTS events CASCADE;
        DROP TABLE IF EXISTS products CASCADE;
        DROP TABLE IF EXISTS categories CASCADE;
        DROP TABLE IF EXISTS clients CASCADE;
        DROP TABLE IF EXISTS campaigns CASCADE;
        DROP TABLE IF EXISTS users CASCADE;
        """
    )


def create_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS clients (
            client_id TEXT PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id),
            user_device_id TEXT,
            first_purchase_date DATE
        );

        CREATE TABLE IF NOT EXISTS campaigns (
            campaign_id BIGINT NOT NULL,
            campaign_type TEXT NOT NULL,
            campaign_key TEXT NOT NULL UNIQUE,
            channel TEXT,
            topic TEXT,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            total_count BIGINT,
            ab_test BOOLEAN,
            warmup_mode BOOLEAN,
            hour_limit DOUBLE PRECISION,
            subject_length DOUBLE PRECISION,
            subject_with_personalization BOOLEAN,
            subject_with_deadline BOOLEAN,
            subject_with_emoji BOOLEAN,
            subject_with_bonuses BOOLEAN,
            subject_with_discount BOOLEAN,
            subject_with_saleout BOOLEAN,
            is_test BOOLEAN,
            position BIGINT,
            PRIMARY KEY (campaign_id, campaign_type)
        );

        CREATE TABLE IF NOT EXISTS categories (
            category_id TEXT PRIMARY KEY,
            category_code TEXT
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id BIGINT PRIMARY KEY,
            category_id TEXT REFERENCES categories(category_id),
            category_code TEXT,
            brand TEXT,
            price NUMERIC(18, 2)
        );

        CREATE TABLE IF NOT EXISTS events (
            event_id BIGSERIAL PRIMARY KEY,
            event_time TIMESTAMPTZ NOT NULL,
            event_type TEXT NOT NULL,
            product_id BIGINT REFERENCES products(product_id),
            category_id TEXT,
            category_code TEXT,
            brand TEXT,
            price NUMERIC(18, 2),
            user_id BIGINT REFERENCES users(user_id),
            user_session TEXT
        );

        CREATE TABLE IF NOT EXISTS friends (
            user_id BIGINT NOT NULL REFERENCES users(user_id),
            friend_id BIGINT NOT NULL REFERENCES users(user_id),
            PRIMARY KEY (user_id, friend_id),
            CHECK (user_id < friend_id)
        );

        CREATE TABLE IF NOT EXISTS messages (
            message_id BIGSERIAL PRIMARY KEY,
            raw_message_id BIGINT,
            campaign_id BIGINT NOT NULL,
            message_type TEXT NOT NULL,
            campaign_key TEXT NOT NULL,
            channel TEXT,
            client_id TEXT REFERENCES clients(client_id),
            user_id BIGINT,
            user_device_id TEXT,
            email_provider TEXT,
            platform TEXT,
            stream TEXT,
            message_date DATE,
            sent_at TIMESTAMPTZ,
            is_opened BOOLEAN,
            opened_first_time_at TIMESTAMPTZ,
            opened_last_time_at TIMESTAMPTZ,
            is_clicked BOOLEAN,
            clicked_first_time_at TIMESTAMPTZ,
            clicked_last_time_at TIMESTAMPTZ,
            is_unsubscribed BOOLEAN,
            unsubscribed_at TIMESTAMPTZ,
            is_hard_bounced BOOLEAN,
            hard_bounced_at TIMESTAMPTZ,
            is_soft_bounced BOOLEAN,
            soft_bounced_at TIMESTAMPTZ,
            is_complained BOOLEAN,
            complained_at TIMESTAMPTZ,
            is_blocked BOOLEAN,
            blocked_at TIMESTAMPTZ,
            is_purchased BOOLEAN,
            purchased_at TIMESTAMPTZ,
            CONSTRAINT fk_messages_campaign
                FOREIGN KEY (campaign_id, message_type)
                REFERENCES campaigns (campaign_id, campaign_type)
        );
        """
    )


def create_indexes(cur) -> None:
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_clients_user_id
            ON clients (user_id);

        CREATE INDEX IF NOT EXISTS idx_campaigns_type_channel
            ON campaigns (campaign_type, channel);

        CREATE INDEX IF NOT EXISTS idx_categories_category_code
            ON categories (category_code);

        CREATE INDEX IF NOT EXISTS idx_products_category_id
            ON products (category_id);

        CREATE INDEX IF NOT EXISTS idx_events_user_time
            ON events (user_id, event_time);

        CREATE INDEX IF NOT EXISTS idx_events_product_id
            ON events (product_id);

        CREATE INDEX IF NOT EXISTS idx_events_event_type
            ON events (event_type);

        CREATE INDEX IF NOT EXISTS idx_friends_user_id
            ON friends (user_id);

        CREATE INDEX IF NOT EXISTS idx_friends_friend_id
            ON friends (friend_id);

        CREATE INDEX IF NOT EXISTS idx_messages_campaign
            ON messages (campaign_id, message_type);

        CREATE INDEX IF NOT EXISTS idx_messages_client_id
            ON messages (client_id);

        CREATE INDEX IF NOT EXISTS idx_messages_user_id
            ON messages (user_id);

        CREATE INDEX IF NOT EXISTS idx_messages_sent_at
            ON messages (sent_at);

        CREATE INDEX IF NOT EXISTS idx_products_fulltext
            ON products
            USING GIN (
                to_tsvector(
                    'simple',
                    coalesce(category_code, '') || ' ' ||
                    regexp_replace(coalesce(category_code, ''), '[._-]+', ' ', 'g') || ' ' ||
                    coalesce(brand, '')
                )
            );
        """
    )


def copy_dataframe(conn, table: str, columns: List[str], df: pd.DataFrame) -> None:
    if df.empty:
        print(f"  {table}: 0 rows")
        return

    buffer = io.StringIO()
    export_df = df[columns].copy()
    export_df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(
            sql.SQL(
                "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
            ).format(
                sql.Identifier(table),
                sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            ).as_string(conn),
            buffer,
        )
    print(f"  {table}: {len(df):,} rows")


def analyze_tables(cur) -> None:
    cur.execute("ANALYZE;")

    cur.execute(
        """
        SELECT relname, n_live_tup
        FROM pg_stat_user_tables
        ORDER BY relname;
        """
    )
    stats = cur.fetchall()
    print("Table statistics:")
    for table_name, row_count in stats:
        print(f"  {table_name}: {row_count:,}")


def main() -> None:
    args = parse_args()
    t0 = time.time()

    frames = prepare_frames(args.data_dir, client_id_prefix=args.client_id_prefix)

    try:
        ensure_database(args)
        conn = connect(args, args.dbname)
    except Exception as exc:  # pragma: no cover - runtime connectivity
        print(f"Could not connect to PostgreSQL: {exc}")
        sys.exit(1)

    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if args.drop:
                print("Dropping old tables...")
                drop_tables(cur)
                conn.commit()

            print("Creating tables...")
            create_tables(cur)
            conn.commit()

        print("Loading dimension and fact tables...")
        copy_dataframe(conn, "users", ["user_id"], frames["users"])
        conn.commit()

        copy_dataframe(
            conn,
            "clients",
            ["client_id", "user_id", "user_device_id", "first_purchase_date"],
            frames["clients"],
        )
        conn.commit()

        copy_dataframe(
            conn,
            "campaigns",
            [
                "campaign_id",
                "campaign_type",
                "campaign_key",
                "channel",
                "topic",
                "started_at",
                "finished_at",
                "total_count",
                "ab_test",
                "warmup_mode",
                "hour_limit",
                "subject_length",
                "subject_with_personalization",
                "subject_with_deadline",
                "subject_with_emoji",
                "subject_with_bonuses",
                "subject_with_discount",
                "subject_with_saleout",
                "is_test",
                "position",
            ],
            frames["campaigns"],
        )
        conn.commit()

        copy_dataframe(
            conn,
            "categories",
            ["category_id", "category_code"],
            frames["categories"],
        )
        conn.commit()

        copy_dataframe(
            conn,
            "products",
            ["product_id", "category_id", "category_code", "brand", "price"],
            frames["products"],
        )
        conn.commit()

        copy_dataframe(
            conn,
            "events",
            [
                "event_time",
                "event_type",
                "product_id",
                "category_id",
                "category_code",
                "brand",
                "price",
                "user_id",
                "user_session",
            ],
            frames["events"],
        )
        conn.commit()

        copy_dataframe(
            conn,
            "friends",
            ["user_id", "friend_id"],
            frames["friends"],
        )
        conn.commit()

        copy_dataframe(
            conn,
            "messages",
            [
                "raw_message_id",
                "campaign_id",
                "message_type",
                "campaign_key",
                "channel",
                "client_id",
                "user_id",
                "user_device_id",
                "email_provider",
                "platform",
                "stream",
                "message_date",
                "sent_at",
                "is_opened",
                "opened_first_time_at",
                "opened_last_time_at",
                "is_clicked",
                "clicked_first_time_at",
                "clicked_last_time_at",
                "is_unsubscribed",
                "unsubscribed_at",
                "is_hard_bounced",
                "hard_bounced_at",
                "is_soft_bounced",
                "soft_bounced_at",
                "is_complained",
                "complained_at",
                "is_blocked",
                "blocked_at",
                "is_purchased",
                "purchased_at",
            ],
            frames["messages"],
        )
        conn.commit()

        with conn.cursor() as cur:
            print("Creating indexes...")
            create_indexes(cur)
            conn.commit()

            analyze_tables(cur)
            conn.commit()

        print(f"PostgreSQL load completed in {time.time() - t0:.1f}s")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
