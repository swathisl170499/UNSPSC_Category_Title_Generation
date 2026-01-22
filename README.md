# UNSPSC Category Title Generation

This project classifies service descriptions into UNSPSC category titles using an OpenAI
completion model, processing input Excel files in configurable batches.

## Requirements

- Python 3.10+
- Dependencies:
  - `pandas`
  - `openai`
  - `openpyxl` (for reading Excel files)

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

If you don't have a `requirements.txt`, you can install directly:

```bash
python -m pip install pandas openai openpyxl
```

## Usage

Provide the input Excel file and (optionally) output path and batch size. The input file must
include:

- `Service Description in English`
- `AI Category Title`

Example:

```bash
export OPENAI_API_KEY="your-key"
python unspsc_classifier.py --input path/to/input.xlsx --output unspsc_output.csv --batch-size 500
```

## Testing

Run the unit tests with:

```bash
python -m pytest
```

## Sandbox Test Run

To validate the pipeline in a sandbox environment, create a small Excel file with a few rows
and run:

```bash
python unspsc_classifier.py --input sample.xlsx --output sandbox_output.csv --batch-size 2
```
