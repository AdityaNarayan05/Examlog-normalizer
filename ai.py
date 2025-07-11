# import json
# import csv
# import sys
# from datetime import datetime

# def main():
#     if len(sys.argv) != 3:
#         print("Usage: python json_to_csv_filter.py <input_file.json> <output_file.csv>")
#         sys.exit(1)

#     input_file = sys.argv[1]
#     output_file = sys.argv[2]

#     # Define the target date string in the format from the JSON
#     target_date_str = "01-06-25"

#     try:
#         # Load JSON data
#         with open(input_file, 'r') as f:
#             data = json.load(f)

#         # Filter data for records that match the target date
#         filtered_data = [record for record in data if record.get("time", "").startswith(target_date_str)]

#         if not filtered_data:
#             print(f"No records found for date: {target_date_str}")
#             return

#         # Get CSV headers from keys of the first record
#         headers = filtered_data[0].keys()

#         # Write to CSV
#         with open(output_file, 'w', newline='') as f:
#             writer = csv.DictWriter(f, fieldnames=headers)
#             writer.writeheader()
#             writer.writerows(filtered_data)

#         print(f"Filtered data for {target_date_str} written to {output_file}")

#     except Exception as e:
#         print(f"Error: {e}")

# if __name__ == "__main__":
#     main()


import json
import csv
import sys
from datetime import datetime

def main():
    if len(sys.argv) != 3:
        print("Usage: python json_to_csv_filter.py <input_file.json> <output_file.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Define start and end dates
    start_date = datetime.strptime("23-05-25", "%d-%m-%y")
    end_date = datetime.strptime("01-06-25", "%d-%m-%y")

    try:
        # Load JSON data
        with open(input_file, 'r') as f:
            data = json.load(f)

        # Filter data for records within the date range
        filtered_data = []
        for record in data:
            time_str = record.get("time", "")
            try:
                record_date = datetime.strptime(time_str, "%d-%m-%y")
                if start_date <= record_date <= end_date:
                    filtered_data.append(record)
            except ValueError:
                continue  # Skip records with invalid date format

        if not filtered_data:
            print(f"No records found between {start_date.strftime('%d-%m-%y')} and {end_date.strftime('%d-%m-%y')}")
            return

        # Get CSV headers from keys of the first record
        headers = filtered_data[0].keys()

        # Write to CSV
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(filtered_data)

        print(f"Filtered data from {start_date.strftime('%d-%m-%y')} to {end_date.strftime('%d-%m-%y')} written to {output_file}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
