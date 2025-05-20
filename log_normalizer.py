import os
import re
import csv
import argparse

# Setup argument parser
parser = argparse.ArgumentParser(description="Normalize firewall log files to CSV format.")
parser.add_argument("input_folder", help="Path to the folder containing log files")
parser.add_argument("output_csv", help="Path to the output CSV file")

args = parser.parse_args()

input_folder = args.input_folder
output_csv = args.output_csv

# Output CSV headers
headers = ["Date Time", "Center Name", "SRC", "DST", "Note", "CAT", "MSG1", "MSG2"]

# Allowed CAT values
allowed_categories = {"WEB FORWARD", "WEB BLOCK", "WEB WARNING"}

# Regex patterns
src_pattern = re.compile(r'src="([^"]+)"')
dst_pattern = re.compile(r'dst="([^"]+)"')
note_pattern = re.compile(r'note="([^"]+)"')
cat_pattern = re.compile(r'cat="([^"]+)"')
msg_pattern = re.compile(r'msg="([^"]+)"')

# Container for CSV rows
rows = []

# Process each file in the input folder
for filename in os.listdir(input_folder):
    filepath = os.path.join(input_folder, filename)
    if os.path.isfile(filepath):
        with open(filepath, 'r') as file:
            for line in file:
                parts = line.strip().split()
                if len(parts) < 4:
                    continue  # Skip malformed lines
                date_time = ' '.join(parts[:3])
                center_name = parts[3]

                # Extract fields
                src = src_pattern.search(line)
                dst = dst_pattern.search(line)
                note = note_pattern.search(line)
                cat = cat_pattern.search(line)
                msg = msg_pattern.search(line)

                src_val = src.group(1) if src else ''
                dst_val = dst.group(1) if dst else ''
                note_val = note.group(1) if note else ''
                cat_val = cat.group(1) if cat and cat.group(1) in allowed_categories else ''

                msg1, msg2 = '', ''
                if msg:
                    msg_content = msg.group(1)
                    if ':' in msg_content:
                        msg1 = msg_content.split(':')[0].strip()
                        remainder = msg_content.split(':', 1)[1]
                        msg2 = remainder.split(',', 1)[0].strip() if ',' in remainder else ''

                rows.append([date_time, center_name, src_val, dst_val, note_val, cat_val, msg1, msg2])

# Write output CSV
with open(output_csv, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers)
    writer.writerows(rows)

print(f"✅ Output written to: {output_csv}")
