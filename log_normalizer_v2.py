import os
import re
import csv
import argparse
from multiprocessing import Process, Queue, cpu_count
from queue import Empty

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


# Worker process to process a list of files
def worker(file_list, queue):
    for filepath in file_list:
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    row = extract_info(line)
                    if row:
                        queue.put(row)
        except Exception as e:
            print(f"⚠️ Error in file {filepath}: {e}")
    queue.put(None)  # Signal done

def writer_process(queue, output_path, num_workers):
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(HEADERS)
        active_workers = 0

        while True:
            try:
                item = queue.get(timeout=30)
                if item is None:
                    active_workers += 1
                    if active_workers == num_workers:
                        break
                else:
                    writer.writerow(item)
            except Empty:
                continue



def chunkify(lst, n):
    """Split list into n chunks"""
    return [lst[i::n] for i in range(n)]


def get_all_files(root_dir):
    """Recursively collect all files under root_dir"""
    all_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            all_files.append(os.path.join(dirpath, fname))
    return all_files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Massive Log Normalizer with Multiprocessing")
    parser.add_argument("root_folder", help="Path to folder with many subfolders of logs")
    parser.add_argument("output_csv", help="Final output CSV path")
    parser.add_argument("--workers", type=int, default=cpu_count()-2, help="Number of parallel workers (default: CPU count - 2)")
    args = parser.parse_args()

    root_dir = args.root_folder
    output_csv = args.output_csv
    num_workers = args.workers

    print(f"📂 Scanning all log files in: {root_dir}")
    all_files = get_all_files(root_dir)
    file_chunks = chunkify(all_files, num_workers)

    print(f"⚙️ Starting {num_workers} workers for {len(all_files)} files...")

    # Create a multiprocessing queue and start writer
    queue = Queue(maxsize=10000)
    # writer = Process(target=writer_process, args=(queue, output_csv))
    writer = Process(target=writer_process, args=(queue, output_csv, num_workers))
    writer.start()

    # Start workers
    processes = []
    for chunk in file_chunks:
        p = Process(target=worker, args=(chunk, queue))
        p.start()
        processes.append(p)

    # Wait for all workers to finish
    for p in processes:
        p.join()

    # Finally join the writer
    writer.join()

    print(f"✅ Done! Normalized CSV created at: {output_csv}")
