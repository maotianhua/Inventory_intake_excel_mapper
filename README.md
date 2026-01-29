# Inventory Intake Excel Mapper

A small CLI tool for filling multiple target Excel files by extracting data from multiple source Excel files. It uses header name matching (fuzzy) to map columns, removes rows with zero values, and fills blanks with `N/A`.

## Install

```bash
pip install -e .
```

## Usage

```bash
excel-mapper \
  --sources "sources/*.xlsx" \
  --targets "targets/*.xlsx" \
  --output-dir output
```

### Common options

- `--min-score 85` Adjust header match sensitivity (0-100)
- `--drop-zero-rows all|any|off` Drop rows with zero values (default: `all`)
- `--fill "N/A"` Fill blanks with a custom value
- `--key "Item ID"` Merge rows on a key column (updates existing rows)
- `--append` Append mapped rows to existing rows
- `--inplace` Write directly into target files
- `--source-sheet "Sheet1"` Read only a named sheet from each source file
- `--target-sheet "Template"` Write only to a named sheet in each target file

## Notes

- The tool preserves target workbook formatting by editing cells in-place.
- Header detection scans the first 20 rows by default; adjust with `--header-scan-rows`.
- If a target header does not find a match above `--min-score`, the column will be filled with `N/A`.
