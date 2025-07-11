import json
import csv
import sys
from datetime import datetime

def main():
    if len(sys.argv) != 3:
        print("Usage: python json_to_csv_filter_june1.py <input_file.json> <output_file.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Target date in DD/MM/YYYY format for filtering
    target_date = "01/06/2025"

    try:
        with open(input_file, 'r') as f:
            data = json.load(f)

        # Filter records where 'date_from' starts with 01/06/2025
        filtered_data = [
            record for record in data
            if record.get("date_from", "").startswith(target_date)
        ]

        if not filtered_data:
            print(f"No records found for date: {target_date}")
            return

        # Get CSV headers from first record
        headers = filtered_data[0].keys()

        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(filtered_data)

        print(f"Filtered data for {target_date} written to {output_file}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()