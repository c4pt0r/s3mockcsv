#!/usr/bin/env python3

import boto3
import pandas as pd
import io
import random
import string
import json
import time
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor

# AWS S3 Configuration
s3 = boto3.client("s3")

# Data Generation Configuration
ROW_SIZE_BYTES = 200  # Approximate size of each row in bytes (id + blob)
BATCH_SIZE = 500_000  # Number of rows per batch

# Thread lock for concurrent writes
lock = threading.Lock()

def generate_blob_data(num_rows, worker_id, start_index):
    """Generate a batch of data with 'id' and 'blob' columns."""
    ids = [
        f"{worker_id}_{start_index + i}" for i in range(num_rows)
    ]
    blobs = [
        "".join(random.choices(string.ascii_letters + string.digits, k=200)) for _ in range(num_rows)
    ]
    data = {
        "id": ids,
        "blob": blobs
    }
    return pd.DataFrame(data)


def upload_part(upload_id, part_number, data, parts, start_time, total_uploaded):
    """Upload a single part to S3 using multipart upload."""
    with io.StringIO() as csv_buffer:
        data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        start_part_time = time.time()
        response = s3.upload_part(
            Bucket=args.bucket_name,
            Key=args.file_name,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=csv_buffer.getvalue(),
        )
        part_time = time.time() - start_part_time
        part_size = len(csv_buffer.getvalue().encode('utf-8'))  # Size of the part in bytes
        with lock:
            # Update total uploaded size and parts list
            total_uploaded[0] += part_size
            parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
            save_checkpoint(upload_id, parts)
            elapsed_time = time.time() - start_time
            throughput = total_uploaded[0] / elapsed_time if elapsed_time > 0 else 0
            print_progress(total_uploaded[0], throughput)


def save_checkpoint(upload_id, parts):
    """Save checkpoint to S3 for resuming in case of failure."""
    checkpoint_data = {"upload_id": upload_id, "parts": parts}
    s3.put_object(
        Bucket=args.bucket_name,
        Key=f"{args.file_name}_progress.json",
        Body=json.dumps(checkpoint_data),
    )


def load_checkpoint():
    """Load checkpoint from S3 if available."""
    try:
        response = s3.get_object(Bucket=args.bucket_name, Key=f"{args.file_name}_progress.json")
        checkpoint_data = json.loads(response["Body"].read().decode())
        return checkpoint_data["upload_id"], checkpoint_data["parts"]
    except Exception:
        return None, []


def print_progress(total_uploaded, throughput):
    """Print progress information: current size and throughput."""
    uploaded_gb = total_uploaded / (1024 ** 3)  # Convert bytes to GB
    throughput_mb_s = throughput / (1024 ** 2)  # Convert bytes per second to MB per second
    print(f"Uploaded: {uploaded_gb:.2f} GB, Throughput: {throughput_mb_s:.2f} MB/s", end='\r')


def upload_csv_to_s3(file_size_gb, worker_id):
    """Generate large CSV data and upload it to S3 using multipart upload."""
    global args
    upload_id, completed_parts = load_checkpoint()

    if upload_id:
        print(f"Resuming existing upload: {upload_id}")
    else:
        print("Starting a new upload...")
        response = s3.create_multipart_upload(Bucket=args.bucket_name, Key=args.file_name)
        upload_id = response["UploadId"]
        completed_parts = []

    part_number = len(completed_parts) + 1
    total_uploaded = [sum(BATCH_SIZE * ROW_SIZE_BYTES for _ in completed_parts)]  # Total size uploaded so far
    start_time = time.time()

    print(f"Resuming from part {part_number}, already uploaded: {total_uploaded[0] / (1024 ** 3):.2f} GB")

    estimated_rows = (file_size_gb * 1024 ** 3) // ROW_SIZE_BYTES

    # Upload parts in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as executor:  # Adjust worker count for better throughput
        futures = []
        for i in range((estimated_rows // BATCH_SIZE) - len(completed_parts)):
            # Generate data for just the current batch (to prevent memory overflow)
            df = generate_blob_data(BATCH_SIZE, worker_id, i * BATCH_SIZE)
            future = executor.submit(upload_part, upload_id, part_number, df, completed_parts, start_time, total_uploaded)
            futures.append(future)
            part_number += 1

        # Wait for all uploads to complete
        for future in futures:
            future.result()

    # Complete multipart upload
    s3.complete_multipart_upload(
        Bucket=args.bucket_name,
        Key=args.file_name,
        UploadId=upload_id,
        MultipartUpload={"Parts": completed_parts},
    )
    
    print("\nUpload complete! Removing progress checkpoint...")
    s3.delete_object(Bucket=args.bucket_name, Key=f"{args.file_name}_progress.json")


def generate_worker_id():
    """Generate worker ID based on the current thread ID."""
    return f"worker_{threading.get_ident()}"


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Upload large CSV data to S3.")
    parser.add_argument("bucket_name", type=str, help="The name of the S3 bucket.")
    parser.add_argument("file_name", type=str, help="The file name to upload (should end with .csv).")
    parser.add_argument("file_size_gb", type=int, help="The size of the file to upload in GB.")
    
    args = parser.parse_args()

    # Generate worker_id based on thread ID
    worker_id = generate_worker_id()

    # Start the upload process
    upload_csv_to_s3(args.file_size_gb, worker_id)

