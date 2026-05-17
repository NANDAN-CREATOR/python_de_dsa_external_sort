# External Merge Sort — DSA × Data Engineering

> **Module 4 — Sorting Algorithms**
> Sort a 10GB CSV file on a machine with only 512MB RAM, and understand exactly how Spark's shuffle sort works under the hood.

[![CI](https://github.com/YOUR_USERNAME/external-sort-dsa/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/external-sort-dsa/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%20|%203.10%20|%203.11%20|%203.12-blue)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## The Problem

You have a 10GB CSV file. Your machine has 512MB of RAM.  
A standard `sorted()` call is impossible — the file does not fit.

This is the exact problem every distributed compute engine faces at scale. Spark, Hadoop, and PostgreSQL all solve it with the same algorithm: **external merge sort**.

This project implements it from scratch in pure Python — no dependencies — and maps every piece to Spark's shuffle internals.

---

## The Algorithm

```
┌─────────────────────────────────────────────────────────┐
│  PHASE 1 — CHUNK SORT  (Spark: map-side spill)          │
│                                                         │
│  10GB input                                             │
│    ├── Read 512MB chunk → sort in RAM → write chunk_0   │
│    ├── Read 512MB chunk → sort in RAM → write chunk_1   │
│    └── ...                                              │
│                                                         │
│  Result: N sorted chunk files, each ≤ RAM budget        │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  PHASE 2 — K-WAY MERGE  (Spark: reduce-side merge)      │
│                                                         │
│  chunk_0 ──┐                                            │
│  chunk_1 ──┤                                            │
│  chunk_2 ──┼──► Min-Heap ──► sorted output (streamed)   │
│    ...   ──┘                                            │
│                                                         │
│  RAM used: O(k) — one row per chunk in heap at any time │
└─────────────────────────────────────────────────────────┘
```

### Spark Shuffle Mapping

| This project | Spark shuffle |
|---|---|
| Phase 1 chunk files | Map-side spill files on executor disk |
| `chunk_size` rows | `spark.shuffle.spill.fileBuffer` (32KB default) |
| Phase 2 heap merge | `ExternalSorter.mergeSpillsWithTransferTo()` |
| `temp_dir` | `spark.local.dir` (executor local disk path) |
| Number of chunks | Number of spill files per partition |

This is why `spark.sql.shuffle.partitions` matters: more partitions → smaller partitions → fewer spills → less disk I/O.

---

## Project Structure

```
external-sort-dsa/
│
├── external_sort/           ← the library
│   ├── __init__.py          ← public API
│   ├── sorter.py            ← Phase 1 + Phase 2 implementation
│   ├── cli.py               ← command-line interface
│   └── generator.py         ← synthetic data generator
│
├── tests/
│   └── test_sorter.py       ← 28 tests (correctness, edge cases, CLI)
│
├── data/                    ← generated CSV files (gitignored)
├── .github/workflows/ci.yml ← GitHub Actions (test + benchmark)
├── .vscode/                 ← VS Code settings + 4 debug configs
├── pyproject.toml           ← packaging (PEP 517)
└── README.md
```

---

## Quick Start

### 1. Clone and set up

```bash
git clone https://github.com/YOUR_USERNAME/external-sort-dsa.git
cd external-sort-dsa

python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate

pip install pytest            # only dev dependency
```

### 2. Generate a large CSV

```bash
# 1 million rows (~85 MB)
PYTHONPATH=. python -m external_sort.generator \
    --rows 1000000 --output data/employees.csv --size-report
```

### 3. Sort it

```bash
PYTHONPATH=. python -m external_sort.cli \
    --input  data/employees.csv \
    --output data/employees_sorted.csv \
    --column salary \
    --numeric \
    --chunk-size 100000 \
    --verbose
```

Output:
```
  External Merge Sort
  ────────────────────────────────────────
  Input      : data/employees.csv
  Output     : data/employees_sorted.csv
  Column     : salary
  Chunk size : 100,000 rows
  Sort type  : numeric
  Order      : ascending

09:41:23  INFO     Phase 1 — reading in chunks of 100,000 rows
09:41:24  INFO     Phase 1 done  — 1,000,000 rows → 10 sorted chunks
09:41:24  INFO     Phase 2 — merging 10 chunks → output
09:41:25  INFO     Phase 2 done  — merge complete

  ✅ Done!
  ────────────────────────────────────────
  Total rows     : 1,000,000
  Chunks created : 10
  Phase 1 (sort) : 1.231s
  Phase 2 (merge): 0.873s
  Total time     : 2.109s
  Peak RAM est.  : 42.1 MB
```

### 4. Use as a library

```python
from external_sort import sort_csv

stats = sort_csv(
    input_path   = "data/employees.csv",
    output_path  = "data/sorted.csv",
    sort_column  = "salary",
    chunk_size   = 500_000,   # tune to available RAM
    numeric_sort = True,
)
print(f"Sorted {stats.total_rows:,} rows in {stats.total_secs:.1f}s")
print(f"Chunks: {stats.chunk_count}  |  Peak RAM: {stats.peak_ram_bytes/1e6:.0f} MB")
```

### 5. Run tests

```bash
PYTHONPATH=. pytest tests/ -v
# 28 passed ✅
```

---

## CLI Reference

```
PYTHONPATH=. python -m external_sort.cli [OPTIONS]

Required:
  --input  / -i   Input CSV path
  --output / -o   Output CSV path
  --column / -c   Column name to sort by

Options:
  --chunk-size / -s  Rows per chunk (default: 100,000)
  --numeric    / -n  Sort as float instead of string
  --reverse    / -r  Descending order
  --delimiter  / -d  Field delimiter (default: comma)
  --encoding   / -e  File encoding (default: utf-8)
  --temp-dir   / -t  Temp directory for chunk files
  --verbose    / -v  Enable debug logging
```

---

## Simulating the 10GB Use Case

```bash
# Generate ~850 MB file (10M rows)
PYTHONPATH=. python -m external_sort.generator \
    --rows 10000000 --output data/employees_10m.csv

# Sort with 512MB RAM constraint
# 512MB / ~85 bytes per row ≈ 6,000,000 rows per chunk → ~2 chunks
PYTHONPATH=. python -m external_sort.cli \
    --input  data/employees_10m.csv \
    --output data/employees_10m_sorted.csv \
    --column salary --numeric \
    --chunk-size 6000000 \
    --verbose
```

For a true 10GB file, set `--chunk-size` so that `chunk_size × avg_row_bytes ≈ 350MB` (70% of 512MB RAM budget). This leaves 30% headroom for Python overhead and the merge heap.

---

## Tuning `chunk_size`

```
chunk_size × avg_row_bytes ≈ peak RAM per run

Examples:
  100,000 × 100B = ~10 MB   → 100 chunks, slower merge
  500,000 × 100B = ~50 MB   → 20 chunks, fast merge
  5,000,000 × 100B = ~500MB → 2 chunks, fastest (if RAM allows)
```

**Rule of thumb:** `chunk_size = (0.7 × available_RAM_bytes) / avg_row_bytes`

---

## DSA Concepts Covered

| Concept | Where |
|---|---|
| Merge sort | `_phase1_chunk_sort` + `_phase2_kway_merge` |
| External memory algorithms | Entire algorithm (data > RAM) |
| Min-heap (priority queue) | `heapq` in `_phase2_kway_merge` |
| K-way merge | `_HeapEntry` + push/pop loop |
| Iterator/streaming pattern | `next(reader, None)` — one row at a time |
| Time complexity O(N log N) | Same as in-memory merge sort |
| Space complexity O(chunk + k) | One chunk + k heap entries |
| Stable sort | Python Timsort in Phase 1 |

---

## GitHub Deploy

```bash
# Replace YOUR_USERNAME with your GitHub username first
git init
git add .
git commit -m "feat: Module 4 — External Merge Sort (DSA for Data Engineers)"
git remote add origin https://github.com/YOUR_USERNAME/external-sort-dsa.git
git branch -M main
git push -u origin main
```

CI will run automatically on push — tests across Python 3.9, 3.10, 3.11, 3.12 plus a benchmark smoke test.

---

## Series

| Module | DSA | DE Problem |
|---|---|---|
| 1 | Arrays & Hashing | Deduplication at scale |
| 2 | Sliding Window | Watermark computation |
| 3 | Binary Search | Partition pruning |
| **4** | **External Sort** | **Sort 10GB in 512MB RAM — Spark shuffle internals** |
| 5 | Graphs | Lineage traversal |
| 6 | Dynamic Programming | Query plan optimisation |
