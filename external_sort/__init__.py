"""
external_sort
=============
External merge sort — sort arbitrarily large CSV files using bounded RAM.
Mirrors the exact two-phase algorithm Apache Spark uses for shuffle sort.

Quick start
-----------
    >>> from external_sort import sort_csv
    >>> stats = sort_csv(
    ...     "big.csv", "sorted.csv",
    ...     sort_column="salary", numeric_sort=True
    ... )
    >>> print(f"Done in {stats.total_secs:.1f}s — {stats.chunk_count} chunks merged")
"""

from .sorter import ExternalMergeSorter, SortStats, sort_csv

__all__    = ["ExternalMergeSorter", "SortStats", "sort_csv"]
__version__ = "1.0.0"
__author__  = "Aman"
