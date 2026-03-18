
from __future__ import annotations

import argparse
import os

from common import (
    CLIENT_ID_PREFIX_DEFAULT,
    frame_counts,
    prepare_frames,
    write_frame,
    write_summary,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and standardize the assignment CSV files.",
    )
    parser.add_argument("--data-dir", required=True, help="Directory that contains the raw CSV files.")
    parser.add_argument("--output-dir", required=True, help="Directory where cleaned CSV files will be written.")
    parser.add_argument(
        "--client-id-prefix",
        default=CLIENT_ID_PREFIX_DEFAULT,
        help="Prefix used to derive user_id from client_id when user_device_id is available.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    frames = prepare_frames(args.data_dir, client_id_prefix=args.client_id_prefix)

    os.makedirs(args.output_dir, exist_ok=True)

    write_frame(frames["events"], os.path.join(args.output_dir, "events.csv"))
    write_frame(frames["campaigns"], os.path.join(args.output_dir, "campaigns.csv"))
    write_frame(frames["messages"], os.path.join(args.output_dir, "messages.csv"))
    write_frame(frames["clients"], os.path.join(args.output_dir, "client_first_purchase_date.csv"))
    write_frame(frames["friends"], os.path.join(args.output_dir, "friends.csv"))

    # Derived helper files used by the loaders.
    write_frame(frames["users"], os.path.join(args.output_dir, "users.csv"))
    write_frame(frames["products"], os.path.join(args.output_dir, "products.csv"))
    write_frame(frames["categories"], os.path.join(args.output_dir, "categories.csv"))

    write_summary(frames, args.output_dir)

    print("Cleaning finished.")
    for name, count in frame_counts(frames).items():
        print(f"  {name}: {count:,} rows")
    print(f"Output written to: {args.output_dir}")


if __name__ == "__main__":
    main()
