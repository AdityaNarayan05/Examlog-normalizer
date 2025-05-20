import zipfile
import re
import csv
import argparse
import time
from io import TextIOWrapper

# Output headers
HEADERS = ["Date Time", "Center Name", "SRC", "DST", "Note", "CAT", "MSG1", "MSG2"]
ALLOWED_NOTE = {"WEB FORWARD", "WEB BLOCK", "WEB WARNING"}

# Regex patterns
PATTERNS = {
    'src': re.compile(r'src="([^"]+)"'),
    'dst': re.compile(r'dst="([^"]+)"'),
    'note': re.compile(r'note="([^"]+)"'),
    'cat': re.compile(r'cat="([^"]+)"'),
    'msg': re.compile(r'msg="([^"]+)"'),
}


def extract_info(line):
    parts = line.strip().split()
    if len(parts) < 4:
        return None

    date_time = ' '.join(parts[:3])
    center_name = parts[3]

    def get(pattern):
        match = PATTERNS[pattern].search(line)
        return match.group(1) if match else ''

    cat_val = get('cat')
    note_val = get('note').upper()
    if note_val not in ALLOWED_NOTE:
        return None  # ✅ Skip if category not allowed

    src_val = get('src')
    dst_val = get('dst')
    # note_val = get('note')

    msg1, msg2 = '', ''
    msg_match = PATTERNS['msg'].search(line)
    if msg_match:
        msg_content = msg_match.group(1)
        if ':' in msg_content:
            msg1 = msg_content.split(':')[0].strip()
            remainder = msg_content.split(':', 1)[1]
            msg2 = remainder.split(',', 1)[0].strip() if ',' in remainder else ''

    return [date_time, center_name, src_val, dst_val, note_val, cat_val, msg1, msg2]


def process_zip(zip_path, output_csv_path, buffer_size=1000):
    start_time = time.time()

    buffer = []
    with zipfile.ZipFile(zip_path, 'r') as zf, open(output_csv_path, 'w', newline='', encoding='utf-8') as out_csv:

        writer = csv.writer(out_csv)
        writer.writerow(HEADERS)

        for name in zf.namelist():
            if zf.getinfo(name).is_dir():
                continue  # Skip folders

            print(f"📄 Processing file: {name}")
            with zf.open(name, 'r') as f:
                wrapper = TextIOWrapper(f, encoding='utf-8', errors='ignore')
                for line in wrapper:
                    row = extract_info(line)
                    if row:
                        buffer.append(row)

                    if len(buffer) >= buffer_size:
                        writer.writerows(buffer)
                        buffer.clear()

        # Write remaining lines
        if buffer:
            writer.writerows(buffer)

    end_time = time.time()
    elapsed = end_time - start_time
    print(f"✅ Done! Output written to: {output_csv_path}")
    print(f"🕒 Total Execution Time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream & Normalize Logs from ZIP")
    parser.add_argument("zip_path", help="Path to input ZIP file")
    parser.add_argument("output_csv", help="Path to output CSV file")
    parser.add_argument("--buffer", type=int, default=1000, help="Buffer size for writing (default: 1000 lines)")
    args = parser.parse_args()

    process_zip(args.zip_path, args.output_csv, args.buffer)
