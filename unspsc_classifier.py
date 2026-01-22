import argparse
import datetime
import os
import time
from dataclasses import dataclass
from typing import Callable

import pandas as pd


REQUIRED_COLUMNS = ["Service Description in English", "AI Category Title"]

UNSPSC_PROMPT_TEMPLATE = (
    "You are a UNSPSC classifier. Return ONLY the UNSPSC category title that best matches "
    "the service description. If uncertain, choose the closest relevant title. "
    "Service description: '{service_description}'."
)


@dataclass
class OpenAICompletionClient:
    create_completion: Callable[..., object]


class UnspscClassifier:
    def __init__(self, api_key: str, client: OpenAICompletionClient) -> None:
        self.api_key = api_key
        self.client = client

    def classify(self, service_description: str) -> str:
        if not service_description or not service_description.strip():
            return "No UNSPSC Found"

        response = self.client.create_completion(
            engine="gpt-3.5-turbo-instruct",
            prompt=UNSPSC_PROMPT_TEMPLATE.format(
                service_description=service_description.strip()
            ),
            max_tokens=32,
            temperature=0,
        )
        text = response.choices[0].text.strip()
        return text or "No UNSPSC Found"


def build_openai_client(api_key: str) -> OpenAICompletionClient:
    import openai

    openai.api_key = api_key
    return OpenAICompletionClient(create_completion=openai.Completion.create)


def ensure_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def process_batches(
    df: pd.DataFrame,
    classifier: UnspscClassifier,
    output_csv_filename: str,
    batch_size: int,
) -> None:
    df["AI Category Title"] = df["AI Category Title"].astype(str)
    total_records = len(df)
    num_batches = (total_records + batch_size - 1) // batch_size

    for batch_num in range(num_batches):
        batch_start_time = time.time()
        batch_start_timestamp = datetime.datetime.now()

        start_index = batch_num * batch_size
        end_index = min(start_index + batch_size, total_records)
        df_batch = df.iloc[start_index:end_index]

        for index, row in df_batch.iterrows():
            service_description = row["Service Description in English"]
            unspsc_info = classifier.classify(service_description)
            df.at[index, "AI Category Title"] = unspsc_info

        write_mode = "a" if os.path.exists(output_csv_filename) else "w"
        with open(output_csv_filename, write_mode, newline="", encoding="utf-8") as f:
            df_batch.to_csv(f, header=not f.tell(), index=False)

        batch_end_time = time.time()
        batch_end_timestamp = datetime.datetime.now()
        time_taken = batch_end_time - batch_start_time
        records_processed = end_index - start_index

        print(f"Batch {batch_num + 1} processed.")
        print(f"Started at: {batch_start_timestamp}")
        print(f"Ended at: {batch_end_timestamp}")
        print(f"Time taken for batch: {time_taken:.2f} seconds")
        print(f"Records processed in this batch: {records_processed}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate UNSPSC category titles from service descriptions."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input Excel file.",
    )
    parser.add_argument(
        "--output",
        default="unspsc_classification_output.csv",
        help="Path to the output CSV file.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=650,
        help="Number of records to process per batch.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("OPENAI_API_KEY"),
        help="OpenAI API key (defaults to OPENAI_API_KEY env var).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.api_key:
        raise ValueError("OpenAI API key is required (use --api-key or OPENAI_API_KEY).")

    overall_start_time = time.time()
    overall_start_timestamp = datetime.datetime.now()
    print(f"Overall process started at {overall_start_timestamp}")

    df = pd.read_excel(args.input)
    ensure_columns(df, REQUIRED_COLUMNS)

    classifier = UnspscClassifier(args.api_key, build_openai_client(args.api_key))
    process_batches(df, classifier, args.output, args.batch_size)

    overall_elapsed_time = time.time() - overall_start_time
    print(f"Overall process completed in {overall_elapsed_time:.2f} seconds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
