import pandas as pd
from datetime import datetime
import argparse

# Custom parser with fallback for bad rows
def parse_datetime_safe(date_str):
    try:
        # Add default year to avoid deprecation warning (Python 3.15+)
        return datetime.strptime(f"2024 {date_str.strip()}", "%Y %b %d %H:%M:%S")
    except Exception as e:
        return None  # Will be filtered out later

def main():
    parser = argparse.ArgumentParser(description="Sort CSV by 'Date Time' column")
    parser.add_argument("input_file", help="Path to the input CSV file")
    parser.add_argument("output_file", help="Path to save the sorted CSV file")
    args = parser.parse_args()

    # Load CSV
    df = pd.read_csv(args.input_file)

    # Parse 'Date Time' with error handling
    df["ParsedDateTime"] = df["Date Time"].apply(parse_datetime_safe)

    # Drop rows where parsing failed
    invalid_rows = df[df["ParsedDateTime"].isnull()]
    valid_df = df.dropna(subset=["ParsedDateTime"])

    # Sort valid rows
    sorted_df = valid_df.sort_values(by="ParsedDateTime").drop(columns=["ParsedDateTime"])

    # Save sorted result
    sorted_df.to_csv(args.output_file, index=False)

    # Report
    print(f"✅ Sorted rows saved to: {args.output_file}")
    print(f"⚠️  Skipped {len(invalid_rows)} row(s) due to invalid 'Date Time' format")

    # Optionally: Save invalid rows to a separate file for inspection
    if not invalid_rows.empty:
        invalid_rows.to_csv("skipped_rows2.csv", index=False)
        print("🚫 Skipped rows saved to: skipped_rows.csv")

if __name__ == "__main__":
    main()