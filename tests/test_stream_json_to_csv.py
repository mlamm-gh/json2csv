import csv
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stream_json_to_csv import StreamingJsonToCsv


# Scenario: converting a top-level JSON array of objects yields a CSV with inferred headers and rows.
def test_convert_from_array(tmp_path):
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
    json_file = tmp_path / "array.json"
    json_file.write_text(json.dumps(data), encoding="utf-8")

    converter = StreamingJsonToCsv(str(json_file))
    csv_path = converter.convert()

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    assert reader.fieldnames == ["id", "name"]
    assert rows == [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]


# Scenario: converting NDJSON content preserves all keys discovered across lines in the header.
def test_convert_from_ndjson(tmp_path):
    ndjson_lines = [
        json.dumps({"id": 1, "email": "alice@example.com"}),
        json.dumps({"id": 2, "email": "bob@example.com", "role": "admin"}),
    ]
    json_file = tmp_path / "data.ndjson"
    json_file.write_text("\n".join(ndjson_lines), encoding="utf-8")

    converter = StreamingJsonToCsv(str(json_file))
    csv_path = converter.convert()

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    assert reader.fieldnames == ["id", "email", "role"]
    assert rows == [
        {"id": "1", "email": "alice@example.com", "role": ""},
        {"id": "2", "email": "bob@example.com", "role": "admin"},
    ]


# Scenario: converting a file containing a single JSON object still produces one CSV row.
def test_convert_single_object(tmp_path):
    json_file = tmp_path / "single.json"
    json_file.write_text(json.dumps({"id": 42, "status": "ok"}), encoding="utf-8")

    converter = StreamingJsonToCsv(str(json_file))
    csv_path = converter.convert()

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    assert reader.fieldnames == ["id", "status"]
    assert rows == [{"id": "42", "status": "ok"}]


# Scenario: nested dictionaries are flattened into dot-separated column names when flatten=True.
def test_convert_flattens_nested_fields(tmp_path):
    data = [{"id": 1, "user": {"name": "Alice", "city": "Paris"}}]
    json_file = tmp_path / "nested.json"
    json_file.write_text(json.dumps(data), encoding="utf-8")

    converter = StreamingJsonToCsv(str(json_file))
    csv_path = converter.convert()

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    assert reader.fieldnames == ["id", "user.name", "user.city"]
    assert rows == [
        {"id": "1", "user.name": "Alice", "user.city": "Paris"},
    ]


# Scenario: when the JSON contains primitives the converter falls back to a single "value" column.
def test_convert_handles_primitives(tmp_path):
    data = [1, 2, 3]
    json_file = tmp_path / "primitives.json"
    json_file.write_text(json.dumps(data), encoding="utf-8")

    converter = StreamingJsonToCsv(str(json_file))
    csv_path = converter.convert()

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    assert reader.fieldnames == ["value"]
    assert rows == [
        {"value": "1"},
        {"value": "2"},
        {"value": "3"},
    ]
