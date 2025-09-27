def main():
    # Example usage of the StreamingJsonToCsv converter.
    # To try it: create a JSON file (array or ndjson) and pass the path as an
    # argument when running this script.
    import sys
    from pathlib import Path

    if len(sys.argv) > 1:
        json_path = sys.argv[1]
        from stream_json_to_csv import StreamingJsonToCsv

        conv = StreamingJsonToCsv(json_path)
        out = conv.convert()
        print(f"Wrote CSV to {out}")
    else:
        print("Hello from json2csv! Provide a JSON file path as the first argument to convert it to CSV.")


if __name__ == "__main__":
    main()
