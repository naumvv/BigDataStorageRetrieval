
from __future__ import annotations

import json
import os
import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


CLIENT_ID_PREFIX_DEFAULT = "151591562"
RAW_REQUIRED_FILES = [
    "events.csv",
    "campaigns.csv",
    "messages.csv",
    "client_first_purchase_date.csv",
    "friends.csv",
]

EVENT_TYPES = {"view", "cart", "purchase", "remove_from_cart"}
CAMPAIGN_TYPES = {"bulk", "trigger", "transactional"}

MESSAGE_BOOL_COLUMNS = [
    "is_opened",
    "is_clicked",
    "is_unsubscribed",
    "is_hard_bounced",
    "is_soft_bounced",
    "is_complained",
    "is_blocked",
    "is_purchased",
]

CAMPAIGN_BOOL_COLUMNS = [
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

MESSAGE_TIMESTAMP_COLUMNS = [
    "sent_at",
    "opened_first_time_at",
    "opened_last_time_at",
    "clicked_first_time_at",
    "clicked_last_time_at",
    "unsubscribed_at",
    "hard_bounced_at",
    "soft_bounced_at",
    "complained_at",
    "blocked_at",
    "purchased_at",
]

CAMPAIGN_TIMESTAMP_COLUMNS = [
    "started_at",
    "finished_at",
]

TEXT_COLUMNS_EVENTS = [
    "event_type",
    "category_id",
    "category_code",
    "brand",
    "user_session",
]

TEXT_COLUMNS_MESSAGES = [
    "message_type",
    "channel",
    "client_id",
    "user_device_id",
    "email_provider",
    "platform",
    "stream",
    "campaign_key",
]

TEXT_COLUMNS_CAMPAIGNS = [
    "campaign_type",
    "channel",
    "topic",
    "campaign_key",
]

TEXT_COLUMNS_CLIENTS = [
    "client_id",
    "user_device_id",
]

TEXT_COLUMNS_FRIENDS = []


def ensure_required_files(data_dir: str) -> None:
    missing = [filename for filename in RAW_REQUIRED_FILES if not os.path.exists(os.path.join(data_dir, filename))]
    if missing:
        raise FileNotFoundError(
            f"Missing required files in {data_dir}: {', '.join(missing)}"
        )


def csv_path(data_dir: str, filename: str) -> str:
    path = os.path.join(data_dir, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return path


def read_csv_loose(path: str) -> pd.DataFrame:
    return pd.read_csv(
        path,
        keep_default_na=True,
        na_values=["", "NA", "N/A", "NaN", "nan", "NULL", "null", "None"],
        low_memory=False,
    )


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [
        str(col)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        for col in out.columns
    ]
    return out


def ensure_column(df: pd.DataFrame, target: str, aliases: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    if target in out.columns:
        return out
    for alias in aliases:
        if alias in out.columns:
            out = out.rename(columns={alias: target})
            return out
    return out


def trim_text(value: Any, lowercase: bool = False) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = str(value).strip()
    if text == "":
        return None

    # clean integers represented as floats, e.g. "123.0"
    if re.fullmatch(r"-?\d+\.0", text):
        text = text[:-2]

    return text.lower() if lowercase else text


def normalize_numeric_string(value: Any) -> Optional[str]:
    text = trim_text(value, lowercase=False)
    if text is None:
        return None
    if re.fullmatch(r"-?\d+\.0+", text):
        text = text.split(".", 1)[0]
    return text


def coerce_text(series: pd.Series, lowercase: bool = False) -> pd.Series:
    return series.map(lambda value: trim_text(value, lowercase=lowercase))


def coerce_int(series: pd.Series) -> pd.Series:
    if series.empty:
        return series.astype("Int64")
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype("Int64")


def coerce_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def coerce_bool(series: pd.Series) -> pd.Series:
    mapping = {
        "t": True,
        "true": True,
        "1": True,
        "yes": True,
        "y": True,
        "f": False,
        "false": False,
        "0": False,
        "no": False,
        "n": False,
    }

    def _convert(value: Any) -> Optional[bool]:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass

        if isinstance(value, bool):
            return value

        text = str(value).strip().lower()
        return mapping.get(text)

    return series.map(_convert)


def coerce_timestamp(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def coerce_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.date


def completeness_score(df: pd.DataFrame, columns: Iterable[str]) -> pd.Series:
    cols = [col for col in columns if col in df.columns]
    if not cols:
        return pd.Series([0] * len(df), index=df.index, dtype="int64")
    return df[cols].notna().sum(axis=1)


def first_non_null(series: pd.Series) -> Any:
    for value in series:
        try:
            if pd.notna(value):
                return value
        except Exception:
            if value is not None:
                return value
    return None


def min_non_null_date(series: pd.Series) -> Optional[date]:
    values = [value for value in series if value is not None and not pd.isna(value)]
    if not values:
        return None
    return min(values)


def build_campaign_key(campaign_type: Any, campaign_id: Any) -> Optional[str]:
    campaign_type_text = trim_text(campaign_type, lowercase=True)
    campaign_id_text = normalize_numeric_string(campaign_id)
    if campaign_type_text is None or campaign_id_text is None:
        return None
    return f"{campaign_type_text}::{campaign_id_text}"


def derive_user_id_from_client_id(
    client_id: Any,
    user_device_id: Any,
    prefix: str = CLIENT_ID_PREFIX_DEFAULT,
) -> Optional[int]:
    client_text = normalize_numeric_string(client_id)
    device_text = normalize_numeric_string(user_device_id)
    if client_text is None or device_text is None:
        return None
    if not client_text.startswith(prefix):
        return None
    if device_text and not client_text.endswith(device_text):
        return None

    middle = client_text[len(prefix):]
    if device_text:
        if len(middle) <= len(device_text):
            return None
        middle = middle[: -len(device_text)]

    if middle.isdigit():
        return int(middle)
    return None


def standardize_events(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_columns(df)
    out = ensure_column(out, "event_time", ["timestamp", "time"])
    out = ensure_column(out, "event_type", ["type"])
    out = ensure_column(out, "product_id", ["product"])
    out = ensure_column(out, "category_id", ["category"])
    out = ensure_column(out, "category_code", ["category_path"])
    out = ensure_column(out, "brand", ["product_brand"])
    out = ensure_column(out, "price", ["product_price"])
    out = ensure_column(out, "user_id", ["user"])
    out = ensure_column(out, "user_session", ["session", "session_id"])

    required = ["event_time", "event_type", "product_id", "user_id"]
    missing_required = [col for col in required if col not in out.columns]
    if missing_required:
        raise ValueError(f"events.csv is missing required columns: {', '.join(missing_required)}")

    out["event_time"] = coerce_timestamp(out["event_time"])
    out["event_type"] = coerce_text(out["event_type"], lowercase=True)
    out["product_id"] = coerce_int(out["product_id"])
    out["user_id"] = coerce_int(out["user_id"])

    if "category_id" in out.columns:
        out["category_id"] = coerce_text(out["category_id"], lowercase=False)
    else:
        out["category_id"] = None

    if "category_code" in out.columns:
        out["category_code"] = coerce_text(out["category_code"], lowercase=True)
    else:
        out["category_code"] = None

    if "brand" in out.columns:
        out["brand"] = coerce_text(out["brand"], lowercase=True)
    else:
        out["brand"] = None

    if "price" in out.columns:
        out["price"] = coerce_float(out["price"])
    else:
        out["price"] = None

    if "user_session" in out.columns:
        out["user_session"] = coerce_text(out["user_session"], lowercase=False)
    else:
        out["user_session"] = None

    out = out.dropna(subset=["event_time", "event_type", "product_id", "user_id"])
    out = out[out["event_type"].isin(EVENT_TYPES)]
    out = out[out["price"].isna() | (out["price"] >= 0)]
    out = out.drop_duplicates()

    out = out[
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
        ]
    ].reset_index(drop=True)

    return out


def standardize_campaigns(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_columns(df)
    out = ensure_column(out, "campaign_id", ["id"])
    out = ensure_column(out, "campaign_type", ["type"])

    required = ["campaign_id", "campaign_type"]
    missing_required = [col for col in required if col not in out.columns]
    if missing_required:
        raise ValueError(f"campaigns.csv is missing required columns: {', '.join(missing_required)}")

    out["campaign_id"] = coerce_int(out["campaign_id"])
    out["campaign_type"] = coerce_text(out["campaign_type"], lowercase=True)

    for column in ["channel", "topic"]:
        if column in out.columns:
            out[column] = coerce_text(out[column], lowercase=True)
        else:
            out[column] = None

    for column in CAMPAIGN_TIMESTAMP_COLUMNS:
        if column in out.columns:
            out[column] = coerce_timestamp(out[column])
        else:
            out[column] = None

    for column in CAMPAIGN_BOOL_COLUMNS:
        if column in out.columns:
            out[column] = coerce_bool(out[column])
        else:
            out[column] = None

    for column in ["total_count", "position"]:
        if column in out.columns:
            out[column] = coerce_int(out[column])
        else:
            out[column] = None

    for column in ["hour_limit", "subject_length"]:
        if column in out.columns:
            out[column] = coerce_float(out[column])
        else:
            out[column] = None

    out = out.dropna(subset=["campaign_id", "campaign_type"])
    out = out[out["campaign_type"].isin(CAMPAIGN_TYPES)]
    out = out.drop_duplicates(subset=["campaign_id", "campaign_type"])

    invalid_bulk_dates = (
        (out["campaign_type"] == "bulk")
        & out["started_at"].notna()
        & out["finished_at"].notna()
        & (out["finished_at"] < out["started_at"])
    )
    out = out[~invalid_bulk_dates]

    if "total_count" in out.columns:
        out = out[out["total_count"].isna() | (out["total_count"] >= 0)]

    if "hour_limit" in out.columns:
        out = out[out["hour_limit"].isna() | (out["hour_limit"] >= 0)]

    out["campaign_key"] = [
        build_campaign_key(campaign_type, campaign_id)
        for campaign_type, campaign_id in zip(out["campaign_type"], out["campaign_id"])
    ]

    ordered_columns = [
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
    ]

    return out[ordered_columns].reset_index(drop=True)


def standardize_messages(
    df: pd.DataFrame,
    client_id_prefix: str = CLIENT_ID_PREFIX_DEFAULT,
) -> pd.DataFrame:
    out = normalize_columns(df)
    out = ensure_column(out, "raw_message_id", ["id"])
    out = ensure_column(out, "message_date", ["date"])
    out = ensure_column(out, "campaign_id", ["id_campaign"])
    out = ensure_column(out, "message_type", ["campaign_type", "type"])

    required = ["campaign_id", "message_type", "client_id"]
    missing_required = [col for col in required if col not in out.columns]
    if missing_required:
        raise ValueError(f"messages.csv is missing required columns: {', '.join(missing_required)}")

    if "raw_message_id" in out.columns:
        out["raw_message_id"] = coerce_int(out["raw_message_id"])
    else:
        out["raw_message_id"] = pd.Series([pd.NA] * len(out), dtype="Int64")

    out["campaign_id"] = coerce_int(out["campaign_id"])
    out["message_type"] = coerce_text(out["message_type"], lowercase=True)

    for column in ["channel", "email_provider", "platform", "stream"]:
        if column in out.columns:
            out[column] = coerce_text(out[column], lowercase=True)
        else:
            out[column] = None

    out["client_id"] = coerce_text(out["client_id"], lowercase=False)

    if "user_id" in out.columns:
        out["user_id"] = coerce_int(out["user_id"])
    else:
        out["user_id"] = pd.Series([pd.NA] * len(out), dtype="Int64")

    if "user_device_id" in out.columns:
        out["user_device_id"] = coerce_text(out["user_device_id"], lowercase=False)
    else:
        out["user_device_id"] = None

    if "message_date" in out.columns:
        out["message_date"] = coerce_date(out["message_date"])
    else:
        out["message_date"] = None

    for column in MESSAGE_TIMESTAMP_COLUMNS:
        if column in out.columns:
            out[column] = coerce_timestamp(out[column])
        else:
            out[column] = None

    for column in MESSAGE_BOOL_COLUMNS:
        if column in out.columns:
            out[column] = coerce_bool(out[column])
        else:
            out[column] = None

    # Enrich user_id from client_id + user_device_id when possible.
    if "user_device_id" in out.columns:
        derived_user_ids = [
            derive_user_id_from_client_id(client_id, user_device_id, prefix=client_id_prefix)
            for client_id, user_device_id in zip(out["client_id"], out["user_device_id"])
        ]
        derived_series = pd.Series(derived_user_ids, index=out.index, dtype="Int64")
        out["user_id"] = out["user_id"].fillna(derived_series)

    if "message_date" in out.columns and "sent_at" in out.columns:
        sent_dates = out["sent_at"].dt.date
        out["message_date"] = out["message_date"].fillna(sent_dates)

    out["campaign_key"] = [
        build_campaign_key(message_type, campaign_id)
        for message_type, campaign_id in zip(out["message_type"], out["campaign_id"])
    ]

    out = out.dropna(subset=["campaign_id", "message_type", "client_id"])
    if "raw_message_id" in out.columns and out["raw_message_id"].notna().any():
        out = (
            out.assign(_score=completeness_score(out, ["sent_at", "is_opened", "is_clicked", "is_purchased"]))
            .sort_values(["_score", "sent_at"], ascending=[False, True], na_position="last")
            .drop_duplicates(subset=["raw_message_id"], keep="first")
            .drop(columns=["_score"])
        )
    else:
        out = out.drop_duplicates()

    if "sent_at" in out.columns and "opened_first_time_at" in out.columns:
        out = out[
            out["opened_first_time_at"].isna()
            | out["sent_at"].isna()
            | (out["opened_first_time_at"] >= out["sent_at"])
        ]
    if "sent_at" in out.columns and "purchased_at" in out.columns:
        out = out[
            out["purchased_at"].isna()
            | out["sent_at"].isna()
            | (out["purchased_at"] >= out["sent_at"])
        ]
    if "opened_last_time_at" in out.columns and "opened_first_time_at" in out.columns:
        out = out[
            out["opened_last_time_at"].isna()
            | out["opened_first_time_at"].isna()
            | (out["opened_last_time_at"] >= out["opened_first_time_at"])
        ]

    ordered_columns = [
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
    ]

    for column in ordered_columns:
        if column not in out.columns:
            out[column] = None

    return out[ordered_columns].reset_index(drop=True)


def standardize_clients(
    df: pd.DataFrame,
    messages: pd.DataFrame,
    client_id_prefix: str = CLIENT_ID_PREFIX_DEFAULT,
) -> pd.DataFrame:
    out = normalize_columns(df)

    # The original public dataset often contains only two columns:
    # client_id and first_purchase_date. The assignment snapshot may also
    # include user_id and user_device_id, so we support both.
    out = ensure_column(
        out,
        "first_purchase_date",
        [
            "date_of_the_first_purchase_ever",
            "date_of_first_purchase_ever",
            "first_purchase_data",
            "purchase_date",
            "date",
        ],
    )

    if "client_id" not in out.columns:
        client_like = [col for col in out.columns if "client" in col]
        if client_like:
            out = out.rename(columns={client_like[0]: "client_id"})

    if "client_id" not in out.columns:
        raise ValueError("client_first_purchase_date.csv is missing client_id")

    out["client_id"] = coerce_text(out["client_id"], lowercase=False)

    if "user_id" in out.columns:
        out["user_id"] = coerce_int(out["user_id"])
    else:
        out["user_id"] = pd.Series([pd.NA] * len(out), dtype="Int64")

    if "user_device_id" in out.columns:
        out["user_device_id"] = coerce_text(out["user_device_id"], lowercase=False)
    else:
        out["user_device_id"] = None

    if "first_purchase_date" in out.columns:
        out["first_purchase_date"] = coerce_date(out["first_purchase_date"])
    else:
        out["first_purchase_date"] = None

    if "user_device_id" in out.columns:
        derived_user_ids = [
            derive_user_id_from_client_id(client_id, user_device_id, prefix=client_id_prefix)
            for client_id, user_device_id in zip(out["client_id"], out["user_device_id"])
        ]
        derived_series = pd.Series(derived_user_ids, index=out.index, dtype="Int64")
        out["user_id"] = out["user_id"].fillna(derived_series)

    # Enrich missing user_id / user_device_id from messages when present.
    message_map = messages[["client_id", "user_id", "user_device_id"]].copy()
    message_map = message_map.dropna(how="all", subset=["user_id", "user_device_id"])
    if not message_map.empty:
        message_map = (
            message_map.groupby("client_id", as_index=False)
            .agg(
                {
                    "user_id": first_non_null,
                    "user_device_id": first_non_null,
                }
            )
        )
        out = out.merge(
            message_map,
            on="client_id",
            how="left",
            suffixes=("", "_msg"),
        )
        if "user_id_msg" in out.columns:
            out["user_id"] = out["user_id"].fillna(out["user_id_msg"])
            out = out.drop(columns=["user_id_msg"])
        if "user_device_id_msg" in out.columns:
            out["user_device_id"] = out["user_device_id"].fillna(out["user_device_id_msg"])
            out = out.drop(columns=["user_device_id_msg"])

    out = out.dropna(subset=["client_id"])

    # Add placeholder clients referenced from messages but missing from the client file.
    missing_clients = messages.loc[
        ~messages["client_id"].isin(out["client_id"]),
        ["client_id", "user_id", "user_device_id"],
    ].drop_duplicates()

    if not missing_clients.empty:
        missing_clients = missing_clients.copy()
        missing_clients["first_purchase_date"] = None
        out = pd.concat(
            [out, missing_clients[["client_id", "user_id", "user_device_id", "first_purchase_date"]]],
            ignore_index=True,
        )

    out = (
        out.groupby("client_id", as_index=False)
        .agg(
            {
                "user_id": first_non_null,
                "user_device_id": first_non_null,
                "first_purchase_date": min_non_null_date,
            }
        )
        .reset_index(drop=True)
    )

    ordered_columns = ["client_id", "user_id", "user_device_id", "first_purchase_date"]
    return out[ordered_columns].reset_index(drop=True)


def standardize_friends(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_columns(df)
    out = ensure_column(out, "user_id", ["friend1", "user_1", "user1"])
    out = ensure_column(out, "friend_id", ["friend2", "user_2", "user2"])

    required = ["user_id", "friend_id"]
    missing_required = [col for col in required if col not in out.columns]
    if missing_required:
        raise ValueError(f"friends.csv is missing required columns: {', '.join(missing_required)}")

    out["user_id"] = coerce_int(out["user_id"])
    out["friend_id"] = coerce_int(out["friend_id"])

    out = out.dropna(subset=["user_id", "friend_id"])
    out = out[out["user_id"] != out["friend_id"]]

    min_ids = out[["user_id", "friend_id"]].min(axis=1)
    max_ids = out[["user_id", "friend_id"]].max(axis=1)
    out["user_id"] = min_ids.astype("Int64")
    out["friend_id"] = max_ids.astype("Int64")

    out = out.drop_duplicates(subset=["user_id", "friend_id"])
    return out[["user_id", "friend_id"]].reset_index(drop=True)


def augment_campaigns_from_messages(
    campaigns: pd.DataFrame,
    messages: pd.DataFrame,
) -> pd.DataFrame:
    if messages.empty:
        return campaigns

    campaign_keys = messages[["campaign_id", "message_type", "campaign_key"]].drop_duplicates()
    existing_keys = campaigns[["campaign_id", "campaign_type", "campaign_key"]].copy()
    existing_keys = existing_keys.rename(columns={"campaign_type": "message_type"})

    merged = campaign_keys.merge(
        existing_keys,
        on=["campaign_id", "message_type", "campaign_key"],
        how="left",
        indicator=True,
    )

    missing = merged.loc[merged["_merge"] == "left_only", ["campaign_id", "message_type", "campaign_key"]]
    if missing.empty:
        return campaigns

    placeholder = pd.DataFrame(
        {
            "campaign_id": missing["campaign_id"],
            "campaign_type": missing["message_type"],
            "campaign_key": missing["campaign_key"],
            "channel": None,
            "topic": None,
            "started_at": None,
            "finished_at": None,
            "total_count": None,
            "ab_test": None,
            "warmup_mode": None,
            "hour_limit": None,
            "subject_length": None,
            "subject_with_personalization": None,
            "subject_with_deadline": None,
            "subject_with_emoji": None,
            "subject_with_bonuses": None,
            "subject_with_discount": None,
            "subject_with_saleout": None,
            "is_test": None,
            "position": None,
        }
    )

    out = pd.concat([campaigns, placeholder], ignore_index=True)
    out = out.drop_duplicates(subset=["campaign_id", "campaign_type"])
    return out.reset_index(drop=True)


def enrich_messages_with_clients(messages: pd.DataFrame, clients: pd.DataFrame) -> pd.DataFrame:
    out = messages.copy()
    if clients.empty:
        return out

    lookup = clients[["client_id", "user_id", "user_device_id"]].copy()
    out = out.merge(lookup, on="client_id", how="left", suffixes=("", "_client"))

    if "user_id_client" in out.columns:
        out["user_id"] = out["user_id"].fillna(out["user_id_client"])
        out = out.drop(columns=["user_id_client"])

    if "user_device_id_client" in out.columns:
        current = out["user_device_id"] if "user_device_id" in out.columns else None
        if current is not None:
            out["user_device_id"] = out["user_device_id"].fillna(out["user_device_id_client"])
        else:
            out["user_device_id"] = out["user_device_id_client"]
        out = out.drop(columns=["user_device_id_client"])

    ordered_columns = [
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
    ]

    return out[ordered_columns].reset_index(drop=True)


def derive_products_and_categories(events: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if events.empty:
        products = pd.DataFrame(columns=["product_id", "category_id", "category_code", "brand", "price"])
        categories = pd.DataFrame(columns=["category_id", "category_code"])
        return products, categories

    working = events.copy().sort_values(["product_id", "event_time"], ascending=[True, False])

    products = (
        working.groupby("product_id", as_index=False)
        .agg(
            {
                "category_id": first_non_null,
                "category_code": first_non_null,
                "brand": first_non_null,
                "price": first_non_null,
            }
        )
        .reset_index(drop=True)
    )

    products = products[["product_id", "category_id", "category_code", "brand", "price"]]

    categories = (
        products.dropna(subset=["category_id"])
        .groupby("category_id", as_index=False)
        .agg({"category_code": first_non_null})
        .reset_index(drop=True)
    )

    return products, categories


def derive_users(
    events: pd.DataFrame,
    clients: pd.DataFrame,
    friends: pd.DataFrame,
    messages: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    user_ids = set()

    for frame, column_names in [
        (events, ["user_id"]),
        (clients, ["user_id"]),
        (friends, ["user_id", "friend_id"]),
        (messages if messages is not None else pd.DataFrame(), ["user_id"]),
    ]:
        if frame is None or frame.empty:
            continue
        for column in column_names:
            if column in frame.columns:
                values = frame[column].dropna().tolist()
                for value in values:
                    try:
                        user_ids.add(int(value))
                    except Exception:
                        pass

    users = pd.DataFrame({"user_id": sorted(user_ids)})
    if not users.empty:
        users["user_id"] = users["user_id"].astype("Int64")
    else:
        users["user_id"] = pd.Series([], dtype="Int64")
    return users


def prepare_frames(
    data_dir: str,
    client_id_prefix: str = CLIENT_ID_PREFIX_DEFAULT,
) -> Dict[str, pd.DataFrame]:
    ensure_required_files(data_dir)

    events = standardize_events(read_csv_loose(csv_path(data_dir, "events.csv")))
    campaigns = standardize_campaigns(read_csv_loose(csv_path(data_dir, "campaigns.csv")))
    messages = standardize_messages(
        read_csv_loose(csv_path(data_dir, "messages.csv")),
        client_id_prefix=client_id_prefix,
    )
    clients = standardize_clients(
        read_csv_loose(csv_path(data_dir, "client_first_purchase_date.csv")),
        messages=messages,
        client_id_prefix=client_id_prefix,
    )
    messages = enrich_messages_with_clients(messages, clients)
    friends = standardize_friends(read_csv_loose(csv_path(data_dir, "friends.csv")))
    campaigns = augment_campaigns_from_messages(campaigns, messages)
    products, categories = derive_products_and_categories(events)
    users = derive_users(events, clients, friends, messages=messages)

    return {
        "events": events,
        "campaigns": campaigns,
        "messages": messages,
        "clients": clients,
        "friends": friends,
        "products": products,
        "categories": categories,
        "users": users,
    }


def pythonize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (pd.Int64Dtype,)):
        return int(value)
    if isinstance(value, pd._libs.missing.NAType):  # type: ignore[attr-defined]
        return None
    if isinstance(value, (pd.Series, pd.DataFrame)):
        raise TypeError("pythonize() expects scalar values only")
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            return value.item()
        except Exception:
            pass
    return value


def records_from_frame(df: pd.DataFrame) -> List[Dict[str, Any]]:
    records = []
    for row in df.to_dict(orient="records"):
        records.append({key: pythonize(value) for key, value in row.items()})
    return records


def write_frame(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)


def frame_counts(frames: Dict[str, pd.DataFrame]) -> Dict[str, int]:
    return {name: int(len(frame)) for name, frame in frames.items()}


def write_summary(frames: Dict[str, pd.DataFrame], output_dir: str) -> None:
    summary = {
        "row_counts": frame_counts(frames),
        "columns": {name: list(frame.columns) for name, frame in frames.items()},
    }
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "cleaning_summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, default=str)
