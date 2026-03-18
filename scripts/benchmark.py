
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shlex
import statistics
import subprocess
import time
from pathlib import Path
from typing import Dict, List


QUERY_FILES = {
    "psql": ["q1.sql", "q2.sql", "q3.sql"],
    "mongodb": ["q2.js", "q3.js"],
    "graph": ["q1.cypher", "q2.cypher", "q3.cypher"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark q1-q3 across PostgreSQL, MongoDB, and Neo4j/Memgraph.",
    )
    parser.add_argument(
        "--scripts-dir",
        default="scripts",
        help="Directory that contains the query files.",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory where benchmark JSON/CSV files will be written.",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=5,
        help="How many times to run each query per database.",
    )
    parser.add_argument(
        "--psql-cmd",
        default="",
        help='Command template for PostgreSQL, e.g. \'psql -h localhost -U postgres -d bigdata_assignment2 -f {query}\'',
    )
    parser.add_argument(
        "--mongo-cmd",
        default="",
        help='Command template for MongoDB, e.g. \'mongosh --file {query}\'',
    )
    parser.add_argument(
        "--graph-cmd",
        default="",
        help='Command template for Neo4j/Memgraph, e.g. \'cypher-shell -u neo4j -p neo4j -f {query}\'',
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue benchmarking other queries if one command fails.",
    )
    return parser.parse_args()


def build_command(template: str, query_path: Path) -> List[str]:
    expanded = template.format(query=str(query_path))
    return shlex.split(expanded)


def summarize(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"mean_seconds": math.nan, "stdev_seconds": math.nan}
    if len(values) == 1:
        return {"mean_seconds": values[0], "stdev_seconds": 0.0}
    return {
        "mean_seconds": statistics.mean(values),
        "stdev_seconds": statistics.stdev(values),
    }


def benchmark_one(command: List[str]) -> Dict[str, object]:
    started = time.perf_counter()
    result = subprocess.run(command, capture_output=True, text=True)
    elapsed = time.perf_counter() - started
    return {
        "returncode": result.returncode,
        "elapsed_seconds": elapsed,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def main() -> None:
    args = parse_args()
    os.environ["PGPASSWORD"] = "postgres"
    os.environ["MONGO_PASSWORD"] = "password"
    os.environ["NEO4J_PASSWORD"] = "password"
    scripts_dir = Path(args.scripts_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    command_templates = {
        "psql": args.psql_cmd.strip(),
        "mongodb": args.mongo_cmd.strip(),
        "graph": args.graph_cmd.strip(),
    }

    all_runs: List[Dict[str, object]] = []
    summary_rows: List[Dict[str, object]] = []

    for engine, query_files in QUERY_FILES.items():
        template = command_templates[engine]
        if not template:
            print(f"Skipping {engine}: no command template was provided.")
            continue

        for query_file in query_files:
            query_path = scripts_dir / query_file
            if not query_path.exists():
                raise FileNotFoundError(f"Missing query file: {query_path}")

            timings: List[float] = []
            print(f"Benchmarking {engine} / {query_file}")

            for run_index in range(1, args.repetitions + 1):
                command = build_command(template, query_path)
                result = benchmark_one(command)
                result_record = {
                    "engine": engine,
                    "query_file": query_file,
                    "run_index": run_index,
                    "command": command,
                    **result,
                }
                all_runs.append(result_record)

                print(
                    f"  run {run_index}: "
                    f"{result['elapsed_seconds']:.6f}s "
                    f"(returncode={result['returncode']})"
                )

                if result["returncode"] != 0:
                    if not args.continue_on_error:
                        raise RuntimeError(
                            f"Command failed for {engine} / {query_file} / run {run_index}.\n"
                            f"stderr:\n{result['stderr']}"
                        )
                else:
                    timings.append(float(result["elapsed_seconds"]))

            stats = summarize(timings)
            summary_rows.append(
                {
                    "engine": engine,
                    "query_file": query_file,
                    "successful_runs": len(timings),
                    "mean_seconds": stats["mean_seconds"],
                    "stdev_seconds": stats["stdev_seconds"],
                }
            )

    (output_dir / "benchmark_runs.json").write_text(json.dumps(all_runs, indent=2))
    (output_dir / "benchmark_summary.json").write_text(json.dumps(summary_rows, indent=2))

    with (output_dir / "benchmark_summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "engine",
                "query_file",
                "successful_runs",
                "mean_seconds",
                "stdev_seconds",
            ],
        )
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Wrote benchmark results to: {output_dir}")


if __name__ == "__main__":
    main()
