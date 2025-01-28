from datetime import datetime
import pandas as pd
import boto3
import os
import base64
import logging

# Initialize AWS clients and environment variables
s3 = boto3.client('s3')
data_bucket = os.environ.get('DATA_BUCKET_NAME')

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
CLICKSTREAM_LABELS = ['prev', 'curr', 'type', 'n']  # Column labels for clickstream data
DELIMITER = '\t'  # Delimiter for splitting input data

def validate_environment_variables():
    """Validate required environment variables."""
    if not data_bucket:
        raise ValueError("Environment variable DATA_BUCKET_NAME is not set.")

def process_record(record):
    """
    Process a single Kinesis Firehose record.
    Args:
        record (dict): A single record from the Kinesis Firehose event.
    Returns:
        dict: Processed record with recordId, result, and data.
    """
    try:
        # Decode the base64-encoded data
        data = base64.b64decode(record.get('data')).decode('utf-8')
        logger.info(f"Processing record: {record['recordId']}")

        # Split data into lines and process each line
        lines = data.split('\n')
        records = []

        for line in lines:
            if not line.strip():  # Skip empty lines
                continue

            try:
                # Split the line into columns using the delimiter
                cols = line.split(DELIMITER)
                if len(cols) != len(CLICKSTREAM_LABELS):  # Validate column count
                    logger.warning(f"Skipping malformed line: {line}")
                    continue

                # Append the processed record
                records.append(cols)
            except Exception as e:
                logger.error(f"Error processing line: {line}. Error: {str(e)}")
                continue

        # Convert records to a Pandas DataFrame
        df = pd.DataFrame(records, columns=CLICKSTREAM_LABELS)

        # Convert DataFrame to CSV and encode it in base64
        csv_data = df.to_csv(header=False, index=False).encode('utf-8')
        encoded_data = base64.b64encode(csv_data).decode('utf-8')

        return {
            "recordId": record['recordId'],
            "result": "Ok",
            "data": encoded_data
        }

    except Exception as e:
        logger.error(f"Error processing record {record['recordId']}: {str(e)}")
        return {
            "recordId": record['recordId'],
            "result": "ProcessingFailed",
            "data": record['data']  # Return original data if processing fails
        }

def handler(event, context):
    """
    Lambda function handler.
    Args:
        event (dict): The event data from Kinesis Firehose.
        context (object): The Lambda context object.
    Returns:
        dict: Response containing processed records.
    """
    validate_environment_variables()

    # Initialize response
    response = {
        "records": []
    }

    # Process each record in the event
    for record in event.get('records', []):
        processed_record = process_record(record)
        response['records'].append(processed_record)

    logger.info(f"Processed {len(response['records'])} records successfully.")
    return response