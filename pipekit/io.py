"""Simple I/O helpers for loading and saving pipeline data."""

import csv
import json
from pathlib import Path
from typing import Any, Union


def load_json(path: Union[str, Path]) -> Any:
    """Load data from a JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Union[str, Path], data: Any, indent: int = 2) -> None:
    """Save data to a JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent)


def load_csv(path: Union[str, Path]) -> list:
    """Load rows from a CSV file as a list of dicts."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def save_csv(path: Union[str, Path], data: list, fieldnames: list = None) -> None:
    """Save a list of dicts to a CSV file."""
    if not data:
        raise ValueError("Cannot save empty data to CSV")
    fieldnames = fieldnames or list(data[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def load_text(path: Union[str, Path]) -> str:
    """Load raw text from a file."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def save_text(path: Union[str, Path], data: str) -> None:
    """Save raw text to a file."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)
