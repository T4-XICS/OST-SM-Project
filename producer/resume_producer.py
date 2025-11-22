#!/usr/bin/env python3
"""
resume_producer.py
Resumes sending CSV rows to Kafka from the last checkpoint and prints progress.
"""
import time
import os
import csv
import argparse
import json
import sys
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--csv", required=True)
parser.add_argument("--topic", required=True)
parser.add_argument("--bootstrap", default="kafka:9092")
parser.add_argument("--checkpoint", default="/app/checkpoint.txt")
parser.add_argument("--flush-every", type=int, default=100)
parser.add_argument("--print-every", type=int, default=10, help="print a short status every N messages")
parser.add_argument("--sleep", type=float, default=0.01, help="delay between messages (seconds)")
args = parser.parse_args()

def read_checkpoint(path):
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                val = f.read().strip()
                if val:
                    return int(val)
        except Exception:
            pass
    return 0

def write_checkpoint(path, lineno):
    try:
        with open(path, "w") as f:
            f.write(str(lineno))
    except Exception as e:
        print("WARN: failed to write checkpoint:", e, file=sys.stderr)

def make_producer(bootstrap):
    # send JSON-encoded values (dict) by default
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def sample_from_row(row, max_items=4):
    # row might be list (csv.reader) or dict (csv.DictReader)
    try:
        if isinstance(row, dict):
            items = list(row.items())[:max_items]
        else:
            # row is list -> show first columns
            items = list(enumerate(row))[:max_items]
        return items
    except Exception:
        return None

def main():
    checkpoint_file = args.checkpoint
    last_sent_line = read_checkpoint(checkpoint_file)
    start_line = last_sent_line + 1

    if not os.path.exists(args.csv):
        print(f"ERROR: CSV file not found: {args.csv}", file=sys.stderr)
        sys.exit(2)

    # count total lines if possible (fast-ish)
    try:
        with open(args.csv, "r", encoding="utf-8", newline="") as f:
            total_lines = sum(1 for _ in f)
    except Exception:
        total_lines = None

    print(f"INFO: starting resume_producer, CSV={args.csv}, start_line={start_line}, topic={args.topic}, bootstrap={args.bootstrap}")
    if total_lines:
        print(f"INFO: CSV total lines: {total_lines}")

    # try DictReader first (so we can send dicts), fallback to reader
    sent = 0
    last_line_processed = last_sent_line
    try:
        producer = make_producer(args.bootstrap)
    except Exception as e:
        print("ERROR: could not create KafkaProducer:", type(e), e, file=sys.stderr)
        sys.exit(3)

    try:
        with open(args.csv, newline='', encoding='utf-8') as csvfile:
            # detect header by trying DictReader and checking fieldnames
            peek = csvfile.readline()
            if not peek:
                print("ERROR: CSV appears empty", file=sys.stderr)
                return
            # rewind
            csvfile.seek(0)
            # create DictReader; if no headers, DictReader will still create fieldnames as first row
            # so we examine first two lines to decide.
            sample_lines = [csvfile.readline() for _ in range(2)]
            csvfile.seek(0)
            has_header = csv.Sniffer().has_header("".join(sample_lines))
            csvfile.seek(0)
            if has_header:
                reader = csv.DictReader(csvfile)
                use_dict = True
            else:
                csvfile.seek(0)
                reader = csv.reader(csvfile)
                use_dict = False

            for i, row in enumerate(reader, start=1):
                last_line_processed = i
                if i < start_line:
                    continue

                # build message payload
                try:
                    if use_dict:
                        payload = row  # already dict
                    else:
                        # send as list (or join) if no header
                        payload = {"row": row}
                except Exception:
                    payload = {"raw": row}

                # send and count
                try:
                    producer.send(args.topic, payload)
                except Exception as e:
                    print("ERROR: send failed at line", i, type(e), e, file=sys.stderr)
                    raise

                sent += 1

                # periodic prints for visibility
                if sent % args.print_every == 0 or i % args.print_every == 0:
                    s = sample_from_row(row)
                    print(f"INFO: sent #{sent} (CSV line {i}) sample: {s}", flush=True)

                # periodic flush & checkpoint
                if sent % args.flush_every == 0:
                    try:
                        producer.flush()
                        write_checkpoint(checkpoint_file, i)
                        print(f"INFO: flushed & checkpointed at CSV line {i} (sent {sent})", flush=True)
                    except Exception as e:
                        print("WARN: flush/checkpoint failed:", e, file=sys.stderr)

                if args.sleep:
                    time.sleep(args.sleep)

    except KeyboardInterrupt:
        print("INFO: interrupted by user, flushing and checkpointing...", flush=True)
    except Exception as e:
        print("ERROR: unexpected exception:", type(e), e, file=sys.stderr)
    finally:
        try:
            producer.flush()
        except Exception:
            pass
        # write final checkpoint (use last_line_processed if > 0)
        if last_line_processed:
            write_checkpoint(checkpoint_file, last_line_processed)
            print(f"INFO: final checkpoint written: {last_line_processed}", flush=True)
        print(f"INFO: finished. total_sent={sent}, last_line={last_line_processed}", flush=True)

if __name__ == "__main__":
    main()
