import os
import pickle
import boto3
import pandas as pd
from io import StringIO
from loguru import logger


# Configuration
checkpoint_file = 'checkpoint.pkl'
chunk_size = int(os.getenv("CHUNKSIZE", 10000))  # Number of rows per chunk
output_dir = 'output_chunks'  # Directory to save the CSV chunks



def get_checkpoint():
    """Retrieve the checkpoint from the file."""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'rb') as f:
            return pickle.load(f)
    return 0

def save_checkpoint(chunk_number):
    """Save the checkpoint to the file."""
    with open(checkpoint_file, 'wb') as f:
        pickle.dump(chunk_number, f)

def read_csv_in_chunks(bucket_name:str, s3_key:str):
    """Read the CSV file from S3 in chunks and save each chunk to the filesystem."""
    # Initialize S3 client
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=s3_key)
    chunk_number = get_checkpoint()
    
    for chunk in pd.read_csv(
        StringIO(response['Body'].read().decode('utf-8')), 
        chunksize=chunk_size, 
        skiprows=range(1, chunk_number * chunk_size + 1),
        on_bad_lines="warn",
    ):
        output_file = os.path.join(output_dir, f'chunk_{chunk_number}.csv')
        
        # Save the chunk to the filesystem
        chunk.to_csv(output_file, index=False)
        
        # Update the checkpoint
        chunk_number += 1
        save_checkpoint(chunk_number)
        
        logger.success(f"Saved chunk {chunk_number}")

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        bucket_name = os.getenv("AWS_BUCKET_NAME")
        s3_key = os.getenv("AWS_S3_KEY")
        read_csv_in_chunks(bucket_name=bucket_name, s3_key=s3_key)
        logger.success("CSV processing completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}. Checkpoint saved. You can re-run the script to continue.")

if __name__ == "__main__":
    main()
