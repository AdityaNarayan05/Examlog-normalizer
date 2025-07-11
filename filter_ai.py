import json
import csv
import sys
from datetime import datetime

def parse_date(date_str):
    """Parses the 'time' field in format 'dd-mm-yy HH:MM:SS'."""
    return datetime.strptime(date_str, "%d-%m-%y %H:%M:%S")

def main():
    if len(sys.argv) != 3:
        print("Usage: python filter_json_to_csv.py input.json output.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Define the time range
    start_date = datetime.strptime("01-05-25", "%d-%m-%y")
    end_date = datetime.strptime("10-05-25", "%d-%m-%y")

    with open(input_file, 'r') as f:
        data = json.load(f)

    # Filter data within the time range
    filtered_data = [
        item for item in data
        if start_date <= parse_date(item["time"]) <= end_date
    ]

    # Define CSV headers
    headers = ["src", "fileName", "category", "time", "unique_id", "subcategory", "dest"]

    # Write filtered data to CSV
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for row in filtered_data:
            writer.writerow(row)

    print(f"Filtered data written to {output_file}")

if __name__ == "__main__":
    main()