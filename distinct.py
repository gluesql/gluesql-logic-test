import re
import sys

def extract_distinct_query_errors(text: str) -> set[str]:
    pattern = r'Error: query failed: gluesql error: (.*)\n'
    matches = re.findall(pattern, text)

    return set(matches)

def extract_from_file(filepath: str) -> set[str]:
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
            return extract_distinct_query_errors(content)
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        return set()
    except Exception as e:
        print(f"Error reading file: {e}")
        return set()

def main():
    filepath = sys.argv[1] if len(sys.argv) > 1 else None

    if not filepath:
        print("Usage: python distinct.py <path_to_error_log>")
        return

    file_errors = extract_from_file(filepath)
    if file_errors:
        for error in sorted(file_errors):
            print(error)

if __name__ == "__main__":
    main()
