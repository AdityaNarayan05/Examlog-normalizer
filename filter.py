# import pandas as pd
# from datetime import datetime, time
# import argparse

# # Convert 'Date Time' to full datetime object (assuming 2024)
# def parse_datetime_safe(date_str):
#     try:
#         return datetime.strptime(f"2024 {date_str.strip()}", "%Y %b %d %H:%M:%S")
#     except:
#         return None

# def is_in_time_window(dt):
#     if dt.date() == datetime(2024, 6, 2).date():
#         t = dt.time()
#         return (time(9, 0) <= t <= time(12, 0)) or (time(15, 0) <= t <= time(18, 0))
#     return False

# def main():
#     parser = argparse.ArgumentParser(description="Filter CSV for June 2 and specific time windows")
#     parser.add_argument("input_file", help="Path to the input CSV file")
#     parser.add_argument("output_file", help="Path to save the filtered CSV file")
#     args = parser.parse_args()

#     # Load data
#     df = pd.read_csv(args.input_file)

#     # Parse Date Time
#     df["ParsedDateTime"] = df["Date Time"].apply(parse_datetime_safe)

#     # Filter out invalid datetimes
#     df = df.dropna(subset=["ParsedDateTime"])

#     # Filter for June 2 and time ranges
#     df_filtered = df[df["ParsedDateTime"].apply(is_in_time_window)]

#     # Drop ParsedDateTime before saving
#     df_filtered = df_filtered.drop(columns=["ParsedDateTime"])

#     # Save to output
#     df_filtered.to_csv(args.output_file, index=False)
#     print(f"✅ Filtered data saved to {args.output_file}")

# if __name__ == "__main__":
#     main()


# import pandas as pd
# from datetime import datetime, time
# import argparse

# # Define your filter criteria here
# FILTER_WINDOWS = {
#     "June 17": [(time(13, 0), time(15, 0))]
#     # "May 15": [(time(9, 0), time(12, 0)), (time(15, 0), time(18, 0))]
# }

# # Parse with assumed year
# def parse_datetime_safe(date_str):
#     try:
#         return datetime.strptime(f"2025 {date_str.strip()}", "%Y %b %d %H:%M:%S")
#     except:
#         return None

# # Check if datetime matches any filter window
# def is_in_any_window(dt):
#     for date_str, windows in FILTER_WINDOWS.items():
#         date_obj = datetime.strptime(f"2025 {date_str}", "%Y %b %d").date()
#         if dt.date() == date_obj:
#             for start, end in windows:
#                 if start <= dt.time() <= end:
#                     return True
#     return False

# def main():
#     parser = argparse.ArgumentParser(description="Filter CSV for specific dates and time windows")
#     parser.add_argument("input_file", help="Path to the input CSV file")
#     parser.add_argument("output_file", help="Path to save the filtered CSV file")
#     args = parser.parse_args()

#     # Load data
#     df = pd.read_csv(args.input_file)

#     # Parse Date Time
#     df["ParsedDateTime"] = df["Date Time"].apply(parse_datetime_safe)

#     # Drop rows where parsing failed
#     df = df.dropna(subset=["ParsedDateTime"])

#     # Apply filtering
#     df_filtered = df[df["ParsedDateTime"].apply(is_in_any_window)]

#     # Drop helper column before saving
#     df_filtered = df_filtered.drop(columns=["ParsedDateTime"])

#     # Save filtered output
#     df_filtered.to_csv(args.output_file, index=False)
#     print(f"✅ Filtered data saved to {args.output_file}")

# if __name__ == "__main__":
#     main()


import pandas as pd
from datetime import datetime, time
import argparse

# Use abbreviated month names for correct parsing
FILTER_WINDOWS = {
    "Jun 18": [(time(12, 0), time(14, 0)), (time(16, 0), time(18, 0))],
    "Jun 19": [(time(12, 0), time(14, 0)), (time(16, 0), time(18, 0))]
}

def parse_datetime_safe(date_str):
    try:
        return datetime.strptime(f"2025 {date_str.strip()}", "%Y %b %d %H:%M:%S")
    except Exception:
        return None

def is_in_any_window(dt):
    for date_str, windows in FILTER_WINDOWS.items():
        try:
            date_obj = datetime.strptime(f"2025 {date_str}", "%Y %b %d").date()
        except Exception:
            continue
        if dt.date() == date_obj:
            for start, end in windows:
                if start <= dt.time() <= end:
                    return True
    return False

def main():
    parser = argparse.ArgumentParser(description="Filter CSV for specific dates and time windows")
    parser.add_argument("input_file", help="Path to the input CSV file")
    parser.add_argument("output_file", help="Path to save the filtered CSV file")
    args = parser.parse_args()

    df = pd.read_csv(args.input_file)
    df["ParsedDateTime"] = df["Date Time"].apply(parse_datetime_safe)
    df = df.dropna(subset=["ParsedDateTime"])
    df_filtered = df[df["ParsedDateTime"].apply(is_in_any_window)]
    df_filtered = df_filtered.drop(columns=["ParsedDateTime"])
    df_filtered.to_csv(args.output_file, index=False)
    print(f"✅ Filtered data saved to {args.output_file}")

if __name__ == "__main__":
    main()
