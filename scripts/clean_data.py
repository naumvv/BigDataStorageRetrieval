import argparse
import os
import sys

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def csv_path(data_dir, filename):
    path = os.path.join(data_dir, filename)
    if not os.path.exists(path):
        print(f"file not found: {path}")
        sys.exit(1)
    return path


def out_path(output_dir, filename):
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, filename)


def report(label, before, after):
    removed = before - after
    print(f"  {label}: {before} -> {after} rows ({removed} removed)")


def parse_bool_col(series):
    mapping = {
        "t": True,
        "f": False,
        "true": True,
        "false": False,
        "1": True,
        "0": False,
    }
    return series.map(
        lambda x: mapping.get(str(x).strip().lower(), None) if pd.notna(x) else None
    )


def clean_events(data_dir, output_dir):
    print("\nevents.csv")
    df = pd.read_csv(
        csv_path(data_dir, "events.csv"),
        parse_dates=["event_time"],
        dtype={"product_id": "Int64", "category_id": str, "user_id": "Int64"},
        low_memory=False,
    )
    before = len(df)

    df = df.dropna(subset=["user_id", "product_id", "event_time"])
    report("drop missing user_id / product_id / event_time", before, len(df))

    before = len(df)
    df = df.drop_duplicates()
    report("drop full duplicates", before, len(df))

    before = len(df)
    valid_types = {"view", "cart", "purchase", "remove_from_cart"}
    df = df[df["event_type"].isin(valid_types)]
    report("drop unknown event_type", before, len(df))

    before = len(df)
    df = df[df["price"].isna() | (df["price"] >= 0)]
    report("drop negative price", before, len(df))

    df["brand"] = df["brand"].str.strip().str.lower()
    df["category_code"] = df["category_code"].str.strip().str.lower()

    df.to_csv(out_path(output_dir, "events.csv"), index=False)
    print(f"  saved: {len(df)} rows")


def clean_campaigns(data_dir, output_dir):
    print("\ncampaigns.csv")
    df = pd.read_csv(
        csv_path(data_dir, "campaigns.csv"),
        parse_dates=["started_at", "finished_at"],
        dtype={"id": "Int64", "total_count": "Int64", "position": "Int64"},
        low_memory=False,
    )
    before = len(df)

    df = df.dropna(subset=["id", "campaign_type"])
    report("drop missing id / campaign_type", before, len(df))

    before = len(df)
    df = df.drop_duplicates(subset=["id", "campaign_type"])
    report("drop duplicate (id, campaign_type)", before, len(df))

    before = len(df)
    valid_types = {"bulk", "trigger", "transactional"}
    df = df[df["campaign_type"].isin(valid_types)]
    report("drop unknown campaign_type", before, len(df))

    before = len(df)
    mask_bulk = df["campaign_type"] == "bulk"
    invalid_dates = (
        mask_bulk
        & df["started_at"].notna()
        & df["finished_at"].notna()
        & (df["finished_at"] < df["started_at"])
    )
    df = df[~invalid_dates]
    report("drop bulk campaigns with finished_at < started_at", before, len(df))

    before = len(df)
    df = df[df["total_count"].isna() | (df["total_count"] >= 0)]
    report("drop negative total_count", before, len(df))

    bool_cols = [
        "ab_test",
        "warmup_mode",
        "subject_with_personalization",
        "subject_with_deadline",
        "subject_with_emoji",
        "subject_with_bonuses",
        "subject_with_discount",
        "subject_with_saleout",
        "is_test",
    ]
    for c in bool_cols:
        if c in df.columns:
            df[c] = parse_bool_col(df[c])

    df.to_csv(out_path(output_dir, "campaigns.csv"), index=False)
    print(f"  saved: {len(df)} rows")


def clean_messages(data_dir, output_dir):
    print("\nmessages.csv")
    bool_cols = [
        "is_opened",
        "is_clicked",
        "is_purchased",
        "is_unsubscribed",
        "is_hard_bounced",
        "is_soft_bounced",
        "is_complained",
        "is_blocked",
    ]
    date_cols = [
        "sent_at",
        "opened_first_time_at",
        "opened_last_time_at",
        "clicked_first_time_at",
        "clicked_last_time_at",
        "purchased_at",
        "unsubscribed_at",
        "hard_bounced_at",
        "soft_bounced_at",
        "complained_at",
        "blocked_at",
    ]
    df = pd.read_csv(
        csv_path(data_dir, "messages.csv"),
        parse_dates=date_cols,
        dtype={"campaign_id": "Int64", "user_id": "Int64", "user_device_id": "Int64"},
        low_memory=False,
    )
    before = len(df)

    df = df.dropna(subset=["client_id", "campaign_id", "sent_at"])
    report("drop missing client_id / campaign_id / sent_at", before, len(df))

    before = len(df)
    df = df.drop_duplicates()
    report("drop full duplicates", before, len(df))

    for c in bool_cols:
        if c in df.columns:
            df[c] = parse_bool_col(df[c])

    before = len(df)
    df = df[
        df["opened_first_time_at"].isna()
        | df["sent_at"].isna()
        | (df["opened_first_time_at"] >= df["sent_at"])
    ]
    report("drop opened_first_time_at < sent_at", before, len(df))

    before = len(df)
    df = df[
        df["purchased_at"].isna()
        | df["sent_at"].isna()
        | (df["purchased_at"] >= df["sent_at"])
    ]
    report("drop purchased_at < sent_at", before, len(df))

    before = len(df)
    df = df[
        df["opened_last_time_at"].isna()
        | df["opened_first_time_at"].isna()
        | (df["opened_last_time_at"] >= df["opened_first_time_at"])
    ]
    report("drop opened_last_time_at < opened_first_time_at", before, len(df))

    df.to_csv(out_path(output_dir, "messages.csv"), index=False)
    print(f"  saved: {len(df)} rows")


def clean_client_first_purchase_date(data_dir, output_dir):
    print("\nclient_first_purchase_date.csv")
    df = pd.read_csv(
        csv_path(data_dir, "client_first_purchase_date.csv"),
        parse_dates=["first_purchase_date"],
        dtype={"user_id": "Int64", "user_device_id": "Int64"},
        low_memory=False,
    )
    before = len(df)

    df = df.dropna(subset=["client_id", "user_id"])
    report("drop missing client_id / user_id", before, len(df))

    before = len(df)
    df = df.drop_duplicates(subset=["client_id"])
    report("drop duplicate client_id", before, len(df))

    before = len(df)
    df = df[
        df["first_purchase_date"].isna()
        | (df["first_purchase_date"] <= pd.Timestamp.now())
    ]
    report("drop future first_purchase_date", before, len(df))

    msgs = pd.read_csv(
        csv_path(data_dir, "messages.csv"),
        dtype={"user_id": "Int64", "user_device_id": "Int64"},
        usecols=["client_id", "user_id", "user_device_id"],
        low_memory=False,
    )
    msgs = msgs.dropna(subset=["client_id"])
    msgs_clients = msgs.drop_duplicates(subset=["client_id"])

    existing_ids = set(df["client_id"].astype(str))
    missing = msgs_clients[
        ~msgs_clients["client_id"].astype(str).isin(existing_ids)
    ].copy()
    missing["client_id"] = missing["client_id"].astype(str)
    missing["first_purchase_date"] = None

    if len(missing) > 0:
        df = pd.concat([df, missing], ignore_index=True)
        print(
            f"  added {len(missing)} missing clients from messages.csv (first_purchase_date=NULL)"
        )

    df.to_csv(out_path(output_dir, "client_first_purchase_date.csv"), index=False)
    print(f"  saved: {len(df)} rows")


def clean_friends(data_dir, output_dir):
    print("\nfriends.csv")
    df = pd.read_csv(
        csv_path(data_dir, "friends.csv"),
        dtype={"friend1": "Int64", "friend2": "Int64"},
    )
    before = len(df)

    df = df.dropna(subset=["friend1", "friend2"])
    report("drop missing friend1 / friend2", before, len(df))

    before = len(df)
    df = df[df["friend1"] != df["friend2"]]
    report("drop self-loops (friend1 == friend2)", before, len(df))

    df["f1"] = df[["friend1", "friend2"]].min(axis=1)
    df["f2"] = df[["friend1", "friend2"]].max(axis=1)
    before = len(df)
    df = df.drop_duplicates(subset=["f1", "f2"])
    report("drop duplicate pairs", before, len(df))
    df = df.drop(columns=["f1", "f2"])

    df.to_csv(out_path(output_dir, "friends.csv"), index=False)
    print(f"  saved: {len(df)} rows")


def main():
    args = parse_args()

    clean_events(args.data_dir, args.output_dir)
    clean_campaigns(args.data_dir, args.output_dir)
    clean_messages(args.data_dir, args.output_dir)
    clean_client_first_purchase_date(args.data_dir, args.output_dir)
    clean_friends(args.data_dir, args.output_dir)

    print("\ndone. cleaned files saved to:", args.output_dir)


if __name__ == "__main__":
    main()
