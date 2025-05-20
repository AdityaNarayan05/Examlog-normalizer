import pandas as pd
import argparse
from collections import Counter, defaultdict
from datetime import datetime
from tqdm import tqdm

def parse_args():
    parser = argparse.ArgumentParser(description="Generate firewall log stats")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Path to output TXT file")
    return parser.parse_args()

def process_file(input_file, output_file, chunksize=100_000):
    total_rows = 0
    center_counter = Counter()
    domain_counter = Counter()
    msg2_counter = Counter()
    note_cat_counter = Counter()
    src_set = set()
    dst_set = set()
    hour_counter = Counter()
    center_domain_cat = defaultdict(lambda: Counter())

    for chunk in tqdm(pd.read_csv(input_file, chunksize=chunksize)):
        total_rows += len(chunk)

        # Update center count
        center_counter.update(chunk["Center Name"])

        # Domain and business type counts
        domain_counter.update(chunk["MSG1"])
        msg2_counter.update(chunk["MSG2"])

        # Source and destination IPs
        src_set.update(chunk["SRC"])
        dst_set.update(chunk["DST"])

        # Note + CAT combos
        note_cat_counter.update(zip(chunk["Note"], chunk["CAT"]))

        # Hourly log counts
        # times = pd.to_datetime(chunk["Date Time"], errors="coerce")
        times = pd.to_datetime(chunk["Date Time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        hours = times.dt.hour.dropna().astype(int)
        hour_counter.update(hours)

        # Extended: Domain Category by Center
        for _, row in chunk.iterrows():
            center = row["Center Name"]
            domain = row["MSG1"]
            cat = row["MSG2"]
            center_domain_cat[center][(domain, cat)] += 1

    # Write report to file
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("=== SUMMARY STATS ===\n")
        f.write(f"Total log entries: {total_rows}\n")
        f.write(f"Unique centers: {len(center_counter)}\n")
        f.write(f"Unique domains (MSG1): {len(domain_counter)}\n")
        f.write(f"Unique business types (MSG2): {len(msg2_counter)}\n")
        f.write(f"Unique source IPs: {len(src_set)}\n")
        f.write(f"Unique destination IPs: {len(dst_set)}\n\n")

        f.write("=== Top 10 Domains ===\n")
        for domain, count in domain_counter.most_common(10):
            f.write(f"{domain}: {count}\n")

        f.write("\n=== MSG2 Type Counts ===\n")
        for msg2, count in msg2_counter.most_common():
            f.write(f"{msg2}: {count}\n")

        f.write("\n=== Log Frequency by Hour ===\n")
        for hour in sorted(hour_counter):
            f.write(f"{hour:02d}:00 - {hour_counter[hour]} entries\n")

        f.write("\n=== Note + CAT Combinations ===\n")
        for (note, cat), count in note_cat_counter.most_common(10):
            f.write(f"{note} + {cat}: {count}\n")

        f.write("\n=== Extended Analysis: Domain Category by Center ===\n")
        for center, domain_cat_counter in center_domain_cat.items():
            f.write(f"\n-- Center: {center} --\n")
            for (domain, cat), count in domain_cat_counter.most_common(5):  # Top 5 per center
                f.write(f"{domain} ({cat}): {count}\n")

    print(f"\n Report written to {output_file}")

if __name__ == "__main__":
    args = parse_args()
    process_file(args.input, args.output)