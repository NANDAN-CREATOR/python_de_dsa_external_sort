"""
external_sort/cli.py
====================
Command-line interface.

    python -m external_sort.cli \\
        --input  data/employees.csv \\
        --output data/employees_sorted.csv \\
        --column salary --numeric --verbose
"""

import argparse
import logging
import sys
from pathlib import Path

from .sorter import sort_csv


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="external-sort",
        description=(
            "External merge sort — sort a CSV of any size using bounded RAM.\n"
            "Phase 1: sort chunks in memory and spill to disk.\n"
            "Phase 2: k-way heap merge across all chunk files.\n\n"
            "Same algorithm as Spark shuffle sort."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input",  "-i", required=True,  help="Input CSV file")
    p.add_argument("--output", "-o", required=True,  help="Output CSV file")
    p.add_argument("--column", "-c", required=True,  help="Column name to sort by")
    p.add_argument("--chunk-size", "-s", type=int, default=100_000,
                   metavar="ROWS",
                   help="Rows per in-memory chunk (default: 100,000)")
    p.add_argument("--numeric", "-n", action="store_true",
                   help="Sort column as numbers (float)")
    p.add_argument("--reverse", "-r", action="store_true",
                   help="Descending order")
    p.add_argument("--delimiter", "-d", default=",",
                   help="CSV delimiter (default: comma)")
    p.add_argument("--encoding", "-e", default="utf-8",
                   help="File encoding (default: utf-8)")
    p.add_argument("--temp-dir", "-t", default=None,
                   help="Temp directory for chunk files")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Debug logging")
    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)

    logging.basicConfig(
        level   = logging.DEBUG if args.verbose else logging.INFO,
        format  = "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt = "%H:%M:%S",
    )

    if not Path(args.input).exists():
        print(f"ERROR: Input file not found: {args.input}", file=sys.stderr)
        return 1

    print(f"\n  External Merge Sort")
    print(f"  {'─' * 40}")
    print(f"  Input      : {args.input}")
    print(f"  Output     : {args.output}")
    print(f"  Column     : {args.column}")
    print(f"  Chunk size : {args.chunk_size:,} rows")
    print(f"  Sort type  : {'numeric' if args.numeric else 'lexicographic'}")
    print(f"  Order      : {'descending' if args.reverse else 'ascending'}")
    print()

    try:
        stats = sort_csv(
            input_path   = args.input,
            output_path  = args.output,
            sort_column  = args.column,
            chunk_size   = args.chunk_size,
            numeric_sort = args.numeric,
            reverse      = args.reverse,
            delimiter    = args.delimiter,
            encoding     = args.encoding,
            temp_dir     = args.temp_dir,
        )
    except ValueError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1

    print(f"\n  ✅ Done!")
    print(f"  {'─' * 40}")
    print(f"  Total rows     : {stats.total_rows:,}")
    print(f"  Chunks created : {stats.chunk_count}")
    print(f"  Phase 1 (sort) : {stats.chunk_sort_secs:.3f}s")
    print(f"  Phase 2 (merge): {stats.merge_secs:.3f}s")
    print(f"  Total time     : {stats.total_secs:.3f}s")
    print(f"  Peak RAM est.  : {stats.peak_ram_bytes / 1_048_576:.1f} MB")
    print(f"  Output         : {args.output}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
