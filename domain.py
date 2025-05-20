import argparse
import csv
import multiprocessing
from multiprocessing import Pool
from functools import partial

# Read CSV in chunks
CHUNK_SIZE = 100000  # Adjust based on your system memory

def process_chunk(rows, msg1_index):
    """Extract unique domains from a chunk of CSV rows."""
    domains = set()
    for row in rows:
        if len(row) > msg1_index:
            domain = row[msg1_index].strip()
            if domain:
                domains.add(domain)
    return domains

def chunked_reader(reader, chunk_size):
    """Yield chunks of rows from the CSV reader."""
    chunk = []
    for row in reader:
        chunk.append(row)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def extract_unique_domains(input_csv, output_csv):
    with open(input_csv, 'r', encoding='utf-8', newline='') as infile:
        reader = csv.reader(infile)
        headers = next(reader)
        
        try:
            msg1_index = headers.index("MSG1")
        except ValueError:
            raise Exception("❌ 'MSG1' column not found in input CSV.")

        pool = Pool(processes=multiprocessing.cpu_count())
        domain_sets = pool.map(partial(process_chunk, msg1_index=msg1_index), chunked_reader(reader, CHUNK_SIZE))
        pool.close()
        pool.join()

        # Merge all domain sets
        unique_domains = set()
        for s in domain_sets:
            unique_domains.update(s)

    # Write output CSV
    with open(output_csv, 'w', encoding='utf-8', newline='') as out_csv:
        writer = csv.writer(out_csv)
        writer.writerow(["Domain"])
        for domain in sorted(unique_domains):
            writer.writerow([domain])

    print(f"✅ Done! Extracted {len(unique_domains)} unique domains.")
    print(f"📄 Output written to: {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract unique domains from MSG1 column in a large CSV.")
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_csv", help="Path to the output CSV file")
    args = parser.parse_args()

    extract_unique_domains(args.input_csv, args.output_csv)