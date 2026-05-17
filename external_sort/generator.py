"""
external_sort/generator.py
===========================
Generate synthetic employee CSV files for benchmarking.

    python -m external_sort.generator --rows 1000000 --output data/employees.csv
"""

import argparse
import csv
import os
import random
import sys
import time
from pathlib import Path

_DEPTS   = ["Engineering", "Sales", "Finance", "HR", "Legal", "Marketing", "Product", "Data"]
_CITIES  = ["New York", "San Francisco", "Austin", "Chicago", "Seattle", "Boston", "Denver"]
_TITLES  = ["Engineer", "Manager", "Analyst", "Director", "Specialist", "Lead", "VP"]
_FIRSTS  = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
            "Linda", "William", "Barbara", "Emma", "Liam", "Aria", "Noah", "Olivia"]


def _rand_name(rng: random.Random) -> str:
    import string
    first = rng.choice(_FIRSTS)
    last  = rng.choice(string.ascii_uppercase) + \
            "".join(rng.choices("abcdefghijklmnopqrstuvwxyz", k=rng.randint(4, 8)))
    return f"{first} {last}"


def generate_csv(output_path: str, num_rows: int, seed: int = 42) -> dict:
    """
    Generate a synthetic employee CSV.

    Columns: employee_id, name, department, job_title, salary,
             city, years_experience, performance_score, hire_date

    Returns dict with rows, file_size, time_secs, output.
    """
    rng = random.Random(seed)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "employee_id", "name", "department", "job_title",
            "salary", "city", "years_experience", "performance_score", "hire_date",
        ])
        for i in range(1, num_rows + 1):
            writer.writerow([
                i,
                _rand_name(rng),
                rng.choice(_DEPTS),
                rng.choice(_TITLES),
                rng.randint(30_000, 350_000),
                rng.choice(_CITIES),
                rng.randint(0, 30),
                round(rng.uniform(1.0, 5.0), 2),
                f"{rng.randint(1995,2024)}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
            ])

    return {
        "rows":      num_rows,
        "file_size": os.path.getsize(output_path),
        "time_secs": time.perf_counter() - t0,
        "output":    output_path,
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="external-sort-generate",
        description="Generate synthetic employee CSV files for benchmarking.",
    )
    p.add_argument("--rows",   "-r", type=int, default=1_000_000)
    p.add_argument("--output", "-o", default="data/employees.csv")
    p.add_argument("--seed",   "-s", type=int, default=42)
    p.add_argument("--size-report", action="store_true")
    args = p.parse_args(argv)

    if args.size_report:
        est = args.rows * 85
        print(f"\n  Estimated size : ~{est / 1_048_576:.0f} MB ({args.rows:,} rows)")
        print(f"  Output         : {args.output}\n")

    print(f"Generating {args.rows:,} rows → {args.output} ...")
    s = generate_csv(args.output, args.rows, args.seed)
    print(f"\n  ✅ Done!")
    print(f"  Rows  : {s['rows']:,}")
    print(f"  Size  : {s['file_size'] / 1_048_576:.1f} MB")
    print(f"  Time  : {s['time_secs']:.1f}s\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
