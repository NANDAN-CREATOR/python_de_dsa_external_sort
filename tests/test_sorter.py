"""
tests/test_sorter.py
====================
Full test suite — correctness, edge cases, multi-chunk, CLI, generator.
"""

import csv
import os
import tempfile
from pathlib import Path

import pytest

from external_sort import ExternalMergeSorter, SortStats, sort_csv
from external_sort.cli import main as cli_main
from external_sort.generator import generate_csv


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

HEADER = ["id", "name", "dept", "salary", "city"]


def write_csv(path: str, rows: list, header: list = None):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if header:
            w.writerow(header)
        w.writerows(rows)


def read_csv(path: str) -> list:
    with open(path, "r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def employee_rows(n: int, seed: int = 0) -> list:
    """Deterministic employee rows for testing."""
    import random
    rng = random.Random(seed)
    depts = ["Eng", "Sales", "HR"]
    cities = ["NYC", "SF", "Austin"]
    return [
        [str(i + 1), f"Emp_{i:04d}", rng.choice(depts),
         str(rng.randint(30_000, 200_000)), rng.choice(cities)]
        for i in range(n)
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Basic correctness
# ─────────────────────────────────────────────────────────────────────────────

class TestBasicCorrectness:

    def test_sort_string_ascending(self, tmp_path):
        rows = [["Zara", "30"], ["Alice", "25"], ["Mike", "35"], ["Bob", "28"]]
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=["name", "age"])

        sort_csv(inp, out, sort_column="name")

        names = [r["name"] for r in read_csv(out)]
        assert names == sorted(names)

    def test_sort_string_descending(self, tmp_path):
        rows = [["Zara", "30"], ["Alice", "25"], ["Mike", "35"], ["Bob", "28"]]
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=["name", "age"])

        sort_csv(inp, out, sort_column="name", reverse=True)

        names = [r["name"] for r in read_csv(out)]
        assert names == sorted(names, reverse=True)

    def test_sort_numeric_ascending(self, tmp_path):
        rows = [["Alice", "300"], ["Bob", "50"], ["Carol", "150"], ["Dave", "25"]]
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=["name", "salary"])

        sort_csv(inp, out, sort_column="salary", numeric_sort=True)

        salaries = [int(r["salary"]) for r in read_csv(out)]
        assert salaries == sorted(salaries)

    def test_sort_numeric_descending(self, tmp_path):
        rows = [["Alice", "300"], ["Bob", "50"], ["Carol", "150"], ["Dave", "25"]]
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=["name", "salary"])

        sort_csv(inp, out, sort_column="salary", numeric_sort=True, reverse=True)

        salaries = [int(r["salary"]) for r in read_csv(out)]
        assert salaries == sorted(salaries, reverse=True)

    def test_header_preserved(self, tmp_path):
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, [["Alice", "25"], ["Bob", "30"]], header=["name", "age"])

        sort_csv(inp, out, sort_column="name")

        with open(out) as fh:
            assert fh.readline().strip() == "name,age"

    def test_row_count_preserved(self, tmp_path):
        rows = employee_rows(500)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        stats = sort_csv(inp, out, sort_column="salary", numeric_sort=True)

        assert stats.total_rows == 500
        assert len(read_csv(out)) == 500

    def test_all_values_preserved(self, tmp_path):
        """Every row in input must appear exactly once in output."""
        rows = employee_rows(200)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        sort_csv(inp, out, sort_column="name")

        original = sorted(r[1] for r in rows)
        result   = sorted(r["name"] for r in read_csv(out))
        assert original == result


# ─────────────────────────────────────────────────────────────────────────────
# Edge cases
# ─────────────────────────────────────────────────────────────────────────────

class TestEdgeCases:

    def test_single_row(self, tmp_path):
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, [["Alice", "25"]], header=["name", "age"])

        stats = sort_csv(inp, out, sort_column="name")

        assert stats.total_rows == 1
        result = read_csv(out)
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_already_sorted(self, tmp_path):
        rows = [["Alice", "10"], ["Bob", "20"], ["Carol", "30"]]
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=["name", "score"])

        sort_csv(inp, out, sort_column="name")

        assert [r["name"] for r in read_csv(out)] == ["Alice", "Bob", "Carol"]

    def test_reverse_sorted_input(self, tmp_path):
        rows = [["Carol", "30"], ["Bob", "20"], ["Alice", "10"]]
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=["name", "score"])

        sort_csv(inp, out, sort_column="name")

        assert [r["name"] for r in read_csv(out)] == ["Alice", "Bob", "Carol"]

    def test_duplicate_sort_keys(self, tmp_path):
        rows = [["Bob", "100"], ["Alice", "200"], ["Bob", "300"], ["Alice", "400"]]
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=["name", "salary"])

        sort_csv(inp, out, sort_column="name")

        names = [r["name"] for r in read_csv(out)]
        assert names[:2] == ["Alice", "Alice"]
        assert names[2:] == ["Bob", "Bob"]

    def test_invalid_column_raises(self, tmp_path):
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, [["Alice", "25"]], header=["name", "age"])

        with pytest.raises(ValueError, match="not found in header"):
            sort_csv(inp, out, sort_column="nonexistent")

    def test_numeric_with_empty_values(self, tmp_path):
        """Empty numeric values should not crash."""
        rows = [["Alice", "100"], ["Bob", ""], ["Carol", "50"]]
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=["name", "salary"])

        sort_csv(inp, out, sort_column="salary", numeric_sort=True)

        assert len(read_csv(out)) == 3  # no crash, all rows preserved


# ─────────────────────────────────────────────────────────────────────────────
# Multi-chunk — the actual external sort behaviour
# ─────────────────────────────────────────────────────────────────────────────

class TestMultiChunk:

    def test_forces_multiple_chunks_numeric(self, tmp_path):
        """chunk_size=10 with 100 rows → 10 chunks → k-way merge."""
        rows = employee_rows(100)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        stats = sort_csv(inp, out, sort_column="salary",
                         numeric_sort=True, chunk_size=10)

        assert stats.chunk_count == 10
        result = [int(r["salary"]) for r in read_csv(out)]
        assert result == sorted(result)

    def test_forces_multiple_chunks_string(self, tmp_path):
        rows = employee_rows(100)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        stats = sort_csv(inp, out, sort_column="name", chunk_size=10)

        assert stats.chunk_count == 10
        names = [r["name"] for r in read_csv(out)]
        assert names == sorted(names)

    def test_multi_chunk_numeric_descending(self, tmp_path):
        rows = employee_rows(100)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        sort_csv(inp, out, sort_column="salary",
                 numeric_sort=True, reverse=True, chunk_size=10)

        result = [int(r["salary"]) for r in read_csv(out)]
        assert result == sorted(result, reverse=True)

    def test_multi_chunk_string_descending(self, tmp_path):
        rows = employee_rows(100)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        sort_csv(inp, out, sort_column="name",
                 reverse=True, chunk_size=10)

        names = [r["name"] for r in read_csv(out)]
        assert names == sorted(names, reverse=True)

    def test_chunk_size_1(self, tmp_path):
        """Extreme: every row is its own chunk."""
        rows = employee_rows(20)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        stats = sort_csv(inp, out, sort_column="name", chunk_size=1)

        assert stats.chunk_count == 20
        names = [r["name"] for r in read_csv(out)]
        assert names == sorted(names)

    def test_chunk_larger_than_file(self, tmp_path):
        """When chunk_size > total rows, only 1 chunk is created."""
        rows = employee_rows(50)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        stats = sort_csv(inp, out, sort_column="name", chunk_size=10_000)

        assert stats.chunk_count == 1
        names = [r["name"] for r in read_csv(out)]
        assert names == sorted(names)

    def test_large_correctness(self, tmp_path):
        """10,000 rows across 20 chunks — full end-to-end correctness check."""
        rows = employee_rows(10_000, seed=77)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        stats = sort_csv(inp, out, sort_column="salary",
                         numeric_sort=True, chunk_size=500)

        assert stats.total_rows == 10_000
        assert stats.chunk_count == 20
        salaries = [int(r["salary"]) for r in read_csv(out)]
        assert salaries == sorted(salaries)


# ─────────────────────────────────────────────────────────────────────────────
# Stats
# ─────────────────────────────────────────────────────────────────────────────

class TestStats:

    def test_stats_fields(self, tmp_path):
        rows = employee_rows(200)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        stats = sort_csv(inp, out, sort_column="name", chunk_size=50)

        assert isinstance(stats, SortStats)
        assert stats.total_rows      == 200
        assert stats.chunk_count     == 4
        assert stats.total_secs      > 0
        assert stats.chunk_sort_secs > 0
        assert stats.merge_secs      > 0
        assert stats.peak_ram_bytes  > 0


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

class TestCLI:

    def test_basic_sort(self, tmp_path):
        rows = employee_rows(100)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        rc = cli_main(["--input", inp, "--output", out, "--column", "name"])

        assert rc == 0
        names = [r["name"] for r in read_csv(out)]
        assert names == sorted(names)

    def test_numeric_sort(self, tmp_path):
        rows = employee_rows(100)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        rc = cli_main(["--input", inp, "--output", out,
                       "--column", "salary", "--numeric"])

        assert rc == 0
        salaries = [int(r["salary"]) for r in read_csv(out)]
        assert salaries == sorted(salaries)

    def test_missing_input_returns_1(self, tmp_path):
        rc = cli_main(["--input", "/no/such/file.csv",
                       "--output", str(tmp_path / "o.csv"),
                       "--column", "name"])
        assert rc == 1

    def test_invalid_column_returns_1(self, tmp_path):
        rows = employee_rows(10)
        inp, out = str(tmp_path / "i.csv"), str(tmp_path / "o.csv")
        write_csv(inp, rows, header=HEADER)

        rc = cli_main(["--input", inp, "--output", out, "--column", "nope"])
        assert rc == 1


# ─────────────────────────────────────────────────────────────────────────────
# Generator
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerator:

    def test_generates_correct_row_count(self, tmp_path):
        out   = str(tmp_path / "emp.csv")
        stats = generate_csv(out, num_rows=1_000)

        assert stats["rows"] == 1_000
        assert Path(out).exists()
        rows = read_csv(out)
        assert len(rows) == 1_000
        assert "salary" in rows[0]

    def test_deterministic_with_same_seed(self, tmp_path):
        a = str(tmp_path / "a.csv")
        b = str(tmp_path / "b.csv")
        generate_csv(a, 200, seed=1)
        generate_csv(b, 200, seed=1)
        assert read_csv(a) == read_csv(b)

    def test_different_seeds_differ(self, tmp_path):
        a = str(tmp_path / "a.csv")
        b = str(tmp_path / "b.csv")
        generate_csv(a, 200, seed=1)
        generate_csv(b, 200, seed=2)
        assert read_csv(a) != read_csv(b)
