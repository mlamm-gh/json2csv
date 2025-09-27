from __future__ import annotations

import csv
import json
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional



class StreamingJsonToCsv:
    """Stream a JSON file and convert it to CSV without loading the whole file into memory.

    Constructor accepts a path to a JSON file. The converter supports two common
    streaming styles:
    - A top-level JSON array: [ {..}, {..}, ... ]  (uses ijson to iterate items)
    - Newline-delimited JSON (NDJSON): one JSON object per line

    Usage:
        conv = StreamingJsonToCsv('data.json')
        conv.convert('out.csv')
    """

    def __init__(self, json_path: str):
        self.json_path = Path(json_path)
        if not self.json_path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.json_path}")

    def _is_array_doc(self) -> bool:
        """Return True if the JSON file looks like a top-level array (starts with '[').

        We only peek at the first non-whitespace character, so this is streaming-friendly.
        """
        with self.json_path.open("r", encoding="utf-8") as fh:
            # skip whitespace
            while True:
                ch = fh.read(1)
                if not ch:
                    return False
                if ch.isspace():
                    continue
                return ch == "["

    def _iter_items_array(self) -> Iterator[Dict]:
        """Iterate items from a top-level JSON array.

        The previous streaming parser proved brittle; for now we parse once
        and yield items to guarantee correctness and keep the API stable.
        """
        with self.json_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)

        if not isinstance(data, list):
            return

        for item in data:
            yield item

    def _iter_items_ndjson(self) -> Iterator[Dict]:
        """Iterate objects from an NDJSON file (one JSON value per line).

        Skips blank lines. Each non-blank line is json.loads()'d.
        """
        with self.json_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

    def _iter_single_object(self) -> Iterator[Dict]:
        """If the file contains a single JSON object (not array), yield it once."""
        with self.json_path.open("r", encoding="utf-8") as fh:
            doc = json.load(fh)
            yield doc

    def _iter_items(self) -> Iterator[Dict]:
        """Dispatch to the appropriate iterator depending on file style."""
        if self._is_array_doc():
            yield from self._iter_items_array()
            return

        try:
            yielded_any = False
            for obj in self._iter_items_ndjson():
                yielded_any = True
                yield obj

            if not yielded_any:
                yield from self._iter_single_object()
        except json.JSONDecodeError:
            # fallback: try treating whole file as one JSON value
            yield from self._iter_single_object()

    @staticmethod
    def _flatten(obj: object, parent_key: str = "", sep: str = ".") -> Dict[str, object]:
        """Flatten nested dicts into dot-separated keys.

        Lists and non-dict values are left as-is (lists are JSON-encoded to string
        when writing CSV to avoid losing structure).
        """
        items: List[tuple[str, object]] = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(StreamingJsonToCsv._flatten(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
        else:
            # not a dict -> represent at the parent key
            items.append((parent_key or "value", obj))
        return dict(items)

    def _gather_fieldnames(self, sample_size: Optional[int], flatten: bool) -> List[str]:
        """Make a single streaming pass to collect fieldnames (union of keys).

        sample_size: if provided, only sample up to that many items. If None,
        scan the entire file (still streaming).
        """
        keys: OrderedDict[str, None] = OrderedDict()
        count = 0
        for obj in self._iter_items():
            if flatten:
                row = self._flatten(obj)
            else:
                if not isinstance(obj, dict):
                    # primitive values -> use a generic column
                    row = {"value": obj}
                else:
                    row = obj

            for k in row.keys():
                keys.setdefault(k, None)

            count += 1
            if sample_size is not None and count >= sample_size:
                break

        return list(keys.keys())

    def convert(
        self,
        csv_path: Optional[str] = None,
        fieldnames: Optional[List[str]] = None,
        flatten: bool = True,
        sample_size: Optional[int] = 1000,
    ) -> Path:
        """Convert the JSON file to CSV.

        - csv_path: destination CSV path. If None, uses the input path with .csv suffix.
        - fieldnames: optional list of csv columns. If omitted, they are inferred by a
          streaming pass over up to `sample_size` items (or the whole file if sample_size is None).
        - flatten: whether to flatten nested dicts into dot-keys.
        - sample_size: number of items to sample when inferring headers. None => full scan.

        Returns the Path to the written CSV file.
        """
        if csv_path is None:
            csv_path = str(self.json_path.with_suffix(".csv"))

        if fieldnames is None:
            fieldnames = self._gather_fieldnames(sample_size, flatten)

        if not fieldnames:
            raise ValueError("No fields discovered for CSV output")

        # Second pass: stream and write rows
        with open(csv_path, "w", newline="", encoding="utf-8") as out_fh:
            writer = csv.DictWriter(out_fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for obj in self._iter_items():
                if flatten:
                    row = self._flatten(obj)
                    # convert lists and dicts to JSON strings for CSV
                    for k, v in list(row.items()):
                        if isinstance(v, (dict, list)):
                            row[k] = json.dumps(v, ensure_ascii=False)
                else:
                    if not isinstance(obj, dict):
                        row = {"value": obj}
                    else:
                        row = obj

                # ensure we only write the requested columns and provide empty string for missing
                out_row = {k: row.get(k, "") for k in fieldnames}
                writer.writerow(out_row)

        return Path(csv_path)
