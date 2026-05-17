"""
external_sort/sorter.py
=======================
External Merge Sort — sort arbitrarily large CSV files
using only a fixed amount of RAM.

Algorithm
---------
Phase 1 (Chunk Sort):
    Read the file in RAM-sized chunks.
    Sort each chunk in memory using Python's Timsort.
    Write sorted chunks to temp files on disk.

Phase 2 (K-Way Merge):
    Open all sorted chunk files simultaneously.
    Use a min-heap to pull the globally smallest row
    across all chunks and stream it to output.
    Only ONE row per chunk lives in memory at any time.

Why this matters for Data Engineers
-------------------------------------
This is EXACTLY how Spark's shuffle sort works:

    This project          |  Spark shuffle
    ----------------------|----------------------------------
    Phase 1 chunk files   |  Map-side spill files
    chunk_size parameter  |  spark.shuffle.spill.fileBuffer
    Phase 2 heap merge    |  ExternalSorter k-way merge
    temp_dir              |  spark.local.dir

Understanding this gives deep intuition for:
  - Why shuffle is expensive (disk I/O in both phases)
  - Why spark.sql.shuffle.partitions matters (more partitions = smaller chunks)
  - Why executor local disk speed matters for sort-heavy workloads
"""

import csv
import heapq
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SortStats:
    """Performance metrics captured during a sort run."""
    input_file:       str   = ""
    output_file:      str   = ""
    total_rows:       int   = 0
    chunk_count:      int   = 0
    chunk_size_rows:  int   = 0
    chunk_sort_secs:  float = 0.0
    merge_secs:       float = 0.0
    total_secs:       float = 0.0
    peak_ram_bytes:   int   = 0


# ─────────────────────────────────────────────────────────────────────────────
# Heap entry — wraps a CSV row for the min-heap
# ─────────────────────────────────────────────────────────────────────────────

class _HeapEntry:
    """
    A single entry in the k-way merge heap.

    Stores:
      key         — the value used for heap ordering
      chunk_index — which chunk file this row came from
      row         — the full CSV row (list of strings)

    Comparison is done only on (key, chunk_index) so that rows
    with equal keys are broken by chunk order (stable-ish).
    The row itself is never compared.
    """
    __slots__ = ("key", "chunk_index", "row")

    def __init__(self, key: Any, chunk_index: int, row: List[str]):
        self.key         = key
        self.chunk_index = chunk_index
        self.row         = row

    def __lt__(self, other: "_HeapEntry") -> bool:
        if self.key != other.key:
            return self.key < other.key
        return self.chunk_index < other.chunk_index

    def __le__(self, other: "_HeapEntry") -> bool:
        return self == other or self < other

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _HeapEntry):
            return NotImplemented
        return self.key == other.key and self.chunk_index == other.chunk_index

    def __gt__(self, other: "_HeapEntry") -> bool:
        return not self <= other

    def __ge__(self, other: "_HeapEntry") -> bool:
        return not self < other


# ─────────────────────────────────────────────────────────────────────────────
# Main sorter class
# ─────────────────────────────────────────────────────────────────────────────

class ExternalMergeSorter:
    """
    Sort a CSV file of arbitrary size using bounded RAM.

    Parameters
    ----------
    sort_column  : name of the column to sort by
    chunk_size   : max rows loaded into RAM per chunk (tune to ~70% of free RAM)
    numeric_sort : if True, parse sort values as float before comparing
    reverse      : if True, sort descending
    delimiter    : CSV field delimiter (default: comma)
    encoding     : file encoding (default: utf-8)
    temp_dir     : directory for temporary chunk files (default: system temp)
    """

    def __init__(
        self,
        sort_column:  str,
        chunk_size:   int  = 100_000,
        numeric_sort: bool = False,
        reverse:      bool = False,
        delimiter:    str  = ",",
        encoding:     str  = "utf-8",
        temp_dir:     Optional[str] = None,
    ):
        self.sort_column  = sort_column
        self.chunk_size   = chunk_size
        self.numeric_sort = numeric_sort
        self.reverse      = reverse
        self.delimiter    = delimiter
        self.encoding     = encoding
        self.temp_dir     = temp_dir

    # ── Public API ────────────────────────────────────────────────────────────

    def sort(self, input_path: str, output_path: str) -> SortStats:
        """Sort input_path → output_path. Returns SortStats."""
        stats = SortStats(input_file=input_path, output_file=output_path)
        t_start = time.perf_counter()

        with tempfile.TemporaryDirectory(dir=self.temp_dir) as tmp:

            # Phase 1 — chunk sort
            t1 = time.perf_counter()
            chunk_files, header, col_idx = self._phase1_chunk_sort(
                input_path, tmp, stats
            )
            stats.chunk_sort_secs = time.perf_counter() - t1

            # Phase 2 — k-way merge
            t2 = time.perf_counter()
            self._phase2_kway_merge(
                chunk_files, header, col_idx, output_path, stats
            )
            stats.merge_secs = time.perf_counter() - t2

        stats.total_secs = time.perf_counter() - t_start
        self._log_stats(stats)
        return stats

    # ── Phase 1 ───────────────────────────────────────────────────────────────

    def _phase1_chunk_sort(
        self,
        input_path: str,
        tmp_dir:    str,
        stats:      SortStats,
    ) -> Tuple[List[str], List[str], int]:
        """
        Read the input CSV in chunks of self.chunk_size rows.
        Sort each chunk in memory and spill to a temp file.

        Returns (chunk_file_paths, header, col_index).
        """
        chunk_files: List[str]       = []
        header:      Optional[List[str]] = None
        col_idx:     int             = -1
        chunk:       List[List[str]] = []
        row_count    = 0

        logger.info(
            f"Phase 1 — reading '{input_path}' "
            f"in chunks of {self.chunk_size:,} rows"
        )

        with open(input_path, "r", encoding=self.encoding, newline="") as fh:
            reader = csv.reader(fh, delimiter=self.delimiter)

            for i, row in enumerate(reader):
                if i == 0:
                    header  = row
                    col_idx = self._column_index(header)
                    logger.info(f"  Sort column : '{self.sort_column}' (index {col_idx})")
                    continue

                chunk.append(row)
                row_count += 1

                if len(chunk) >= self.chunk_size:
                    path = self._spill_chunk(chunk, col_idx, tmp_dir, len(chunk_files), header)
                    chunk_files.append(path)
                    stats.peak_ram_bytes = max(
                        stats.peak_ram_bytes, _estimate_bytes(chunk)
                    )
                    chunk.clear()

            # Final partial chunk
            if chunk:
                path = self._spill_chunk(chunk, col_idx, tmp_dir, len(chunk_files), header)
                chunk_files.append(path)
                stats.peak_ram_bytes = max(
                    stats.peak_ram_bytes, _estimate_bytes(chunk)
                )

        stats.total_rows      = row_count
        stats.chunk_count     = len(chunk_files)
        stats.chunk_size_rows = self.chunk_size
        logger.info(
            f"Phase 1 done  — {row_count:,} rows → {len(chunk_files)} sorted chunks"
        )
        return chunk_files, header, col_idx

    def _spill_chunk(
        self,
        chunk:     List[List[str]],
        col_idx:   int,
        tmp_dir:   str,
        chunk_num: int,
        header:    List[str],
    ) -> str:
        """
        Sort a chunk in memory and write it to a numbered temp file.
        Uses Python's built-in Timsort — O(N log N), stable.
        """
        chunk.sort(
            key     = lambda row: self._sort_key(row[col_idx]),
            reverse = self.reverse,
        )
        path = os.path.join(tmp_dir, f"chunk_{chunk_num:06d}.csv")
        with open(path, "w", encoding=self.encoding, newline="") as fh:
            w = csv.writer(fh, delimiter=self.delimiter)
            w.writerow(header)
            w.writerows(chunk)
        logger.debug(f"  Spilled chunk {chunk_num} ({len(chunk):,} rows) → {path}")
        return path

    # ── Phase 2 ───────────────────────────────────────────────────────────────

    def _phase2_kway_merge(
        self,
        chunk_files: List[str],
        header:      List[str],
        col_idx:     int,
        output_path: str,
        stats:       SortStats,
    ) -> None:
        """
        Merge N sorted chunk files into one sorted output using a min-heap.

        Memory usage: O(k) where k = number of chunks.
        Disk I/O: each chunk file is read exactly once, sequentially.

        This is the SAME algorithm as Spark's ExternalSorter.mergeSpillsWithTransferTo()
        """
        logger.info(
            f"Phase 2 — merging {len(chunk_files)} chunks → '{output_path}'"
        )

        file_handles = [
            open(p, "r", encoding=self.encoding, newline="")
            for p in chunk_files
        ]
        readers = [
            csv.reader(fh, delimiter=self.delimiter)
            for fh in file_handles
        ]

        # Skip headers in chunk files
        for r in readers:
            next(r, None)

        heap: List[_HeapEntry] = []

        try:
            # Seed heap with first row from each chunk
            for i, reader in enumerate(readers):
                row = next(reader, None)
                if row is not None:
                    heapq.heappush(heap, _HeapEntry(
                        key         = self._heap_key(row[col_idx]),
                        chunk_index = i,
                        row         = row,
                    ))

            with open(output_path, "w", encoding=self.encoding, newline="") as out:
                writer = csv.writer(out, delimiter=self.delimiter)
                writer.writerow(header)

                while heap:
                    # Pop globally smallest (or largest if reverse) row
                    entry = heapq.heappop(heap)
                    writer.writerow(entry.row)

                    # Replenish from the same chunk
                    nxt = next(readers[entry.chunk_index], None)
                    if nxt is not None:
                        heapq.heappush(heap, _HeapEntry(
                            key         = self._heap_key(nxt[col_idx]),
                            chunk_index = entry.chunk_index,
                            row         = nxt,
                        ))

        finally:
            for fh in file_handles:
                try:
                    fh.close()
                except Exception:
                    pass

        logger.info("Phase 2 done  — merge complete")

    # ── Key helpers ───────────────────────────────────────────────────────────

    def _sort_key(self, value: str) -> Any:
        """
        Key used in Phase 1 chunk.sort().
        chunk.sort() already accepts reverse=, so we return the raw key.
        """
        if self.numeric_sort:
            try:
                return float(value)
            except (ValueError, TypeError):
                return float("-inf")
        return value

    def _heap_key(self, value: str) -> Any:
        """
        Key used in Phase 2 min-heap.
        The heap always pops the SMALLEST key first.
        For ascending  sort: use the value directly.
        For descending sort: negate numeric / invert string so smallest → emitted first.
        """
        if self.numeric_sort:
            try:
                k = float(value)
            except (ValueError, TypeError):
                k = float("-inf")
            return -k if self.reverse else k
        else:
            return _Inverted(value) if self.reverse else value

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _column_index(self, header: List[str]) -> int:
        try:
            return header.index(self.sort_column)
        except ValueError:
            raise ValueError(
                f"Sort column '{self.sort_column}' not found in header.\n"
                f"Available columns: {header}"
            )

    @staticmethod
    def _log_stats(stats: SortStats) -> None:
        logger.info("=" * 56)
        logger.info("Sort complete")
        logger.info(f"  Total rows       : {stats.total_rows:,}")
        logger.info(f"  Chunks           : {stats.chunk_count}")
        logger.info(f"  Phase 1 (sort)   : {stats.chunk_sort_secs:.3f}s")
        logger.info(f"  Phase 2 (merge)  : {stats.merge_secs:.3f}s")
        logger.info(f"  Total            : {stats.total_secs:.3f}s")
        logger.info(
            f"  Peak RAM est.    : "
            f"{stats.peak_ram_bytes / 1_048_576:.1f} MB"
        )
        logger.info("=" * 56)


# ─────────────────────────────────────────────────────────────────────────────
# _Inverted — reverses string comparison for the heap
# ─────────────────────────────────────────────────────────────────────────────

class _Inverted:
    """
    Wraps a string so that its comparison is the inverse of the original.
    Used to make a min-heap behave as a max-heap for string sort keys.

    Example:
        _Inverted("Zara") < _Inverted("Alice")  →  True
        (because "Zara" > "Alice" in normal order)
    """
    __slots__ = ("value",)

    def __init__(self, value: str):
        self.value = value

    def __lt__(self, other: "_Inverted") -> bool:  return self.value > other.value
    def __le__(self, other: "_Inverted") -> bool:  return self.value >= other.value
    def __gt__(self, other: "_Inverted") -> bool:  return self.value < other.value
    def __ge__(self, other: "_Inverted") -> bool:  return self.value <= other.value
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _Inverted):
            return NotImplemented
        return self.value == other.value


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _estimate_bytes(chunk: List[List[str]]) -> int:
    """Rough RAM estimate for a chunk: 50-byte Python overhead + raw string bytes."""
    return sum(50 + sum(len(v) for v in row) for row in chunk)


# ─────────────────────────────────────────────────────────────────────────────
# Convenience function
# ─────────────────────────────────────────────────────────────────────────────

def sort_csv(
    input_path:   str,
    output_path:  str,
    sort_column:  str,
    chunk_size:   int  = 100_000,
    numeric_sort: bool = False,
    reverse:      bool = False,
    delimiter:    str  = ",",
    encoding:     str  = "utf-8",
    temp_dir:     Optional[str] = None,
) -> SortStats:
    """
    Sort a CSV file of any size using bounded RAM (external merge sort).

    Parameters
    ----------
    input_path   : path to the input CSV file
    output_path  : path to write the sorted output
    sort_column  : column name to sort by (must exist in CSV header)
    chunk_size   : rows per in-memory chunk — tune to ~70% of free RAM
    numeric_sort : sort as numbers (float) instead of strings
    reverse      : descending order
    delimiter    : CSV field separator (default comma)
    encoding     : file encoding (default utf-8)
    temp_dir     : directory for temp chunk files (default: system temp)

    Returns
    -------
    SortStats with timing, row counts, and RAM estimates.

    Example
    -------
    >>> from external_sort import sort_csv
    >>> stats = sort_csv(
    ...     "data/employees_10gb.csv",
    ...     "data/employees_sorted.csv",
    ...     sort_column  = "salary",
    ...     chunk_size   = 500_000,
    ...     numeric_sort = True,
    ... )
    >>> print(f"Sorted {stats.total_rows:,} rows in {stats.total_secs:.1f}s")
    """
    return ExternalMergeSorter(
        sort_column  = sort_column,
        chunk_size   = chunk_size,
        numeric_sort = numeric_sort,
        reverse      = reverse,
        delimiter    = delimiter,
        encoding     = encoding,
        temp_dir     = temp_dir,
    ).sort(input_path, output_path)
