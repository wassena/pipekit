"""Tests for pipekit.io load/save helpers."""

import json
import pytest
from pathlib import Path
from pipekit.io import (
    load_json, save_json,
    load_csv, save_csv,
    load_text, save_text,
)


@pytest.fixture
def tmp(tmp_path):
    return tmp_path


def test_save_and_load_json(tmp):
    data = {"key": "value", "nums": [1, 2, 3]}
    path = tmp / "data.json"
    save_json(path, data)
    loaded = load_json(path)
    assert loaded == data


def test_json_indent_formatting(tmp):
    path = tmp / "pretty.json"
    save_json(path, {"a": 1}, indent=4)
    content = path.read_text()
    assert "\n" in content


def test_save_and_load_csv(tmp):
    data = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
    path = tmp / "data.csv"
    save_csv(path, data)
    loaded = load_csv(path)
    assert loaded == data


def test_save_csv_empty_raises(tmp):
    with pytest.raises(ValueError, match="empty"):
        save_csv(tmp / "empty.csv", [])


def test_save_csv_custom_fieldnames(tmp):
    data = [{"a": 1, "b": 2}]
    path = tmp / "custom.csv"
    save_csv(path, data, fieldnames=["b", "a"])
    content = path.read_text()
    assert content.startswith("b,a")


def test_save_and_load_text(tmp):
    path = tmp / "file.txt"
    save_text(path, "hello world")
    assert load_text(path) == "hello world"


def test_load_json_invalid_path():
    with pytest.raises(FileNotFoundError):
        load_json("/nonexistent/path.json")
