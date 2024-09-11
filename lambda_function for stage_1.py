
import json
import boto3
import pandas as pd
import pymysql
import os
from io import StringIO

# RDS configuration details from environment variables
rds_host = os.environ['RDS_HOST']
rds_user = os.environ['RDS_USER']
rds_password = os.environ['RDS_PASSWORD']
rds_db_name = os.environ['RDS_DB_NAME']

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Log the event object to understand its structure
    print("Event: ", json.dumps(event))
    
    try:
        # Extract bucket and file information from the event
        records = event.get('Records', [])
        if not records:
            raise ValueError("No records found in the event.")
        
        bucket_name = records[0]['s3']['bucket']['name']
        file_key = records[0]['s3']['object']['key']
    except KeyError as e:
        print(f"KeyError: {str(e)}")
        return {"statusCode": 500, "body": json.dumps(f"Error extracting data: {str(e)}")}
    except ValueError as e:
        print(f"ValueError: {str(e)}")
        return {"statusCode": 400, "body": json.dumps(f"Error processing event: {str(e)}")}
    
    # Read the CSV file from S3
    try:
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = s3_response['Body'].read().decode('utf-8')
        
        # Read the CSV data into a DataFrame
        df = pd.read_csv(StringIO(csv_data), parse_dates=['date'], dayfirst=True)
    except Exception as e:
        print(f"Error reading CSV file from S3: {str(e)}")
        return {"statusCode": 500, "body": json.dumps("Failed to read CSV file from S3")}
    
    # Connect to RDS
    try:
        connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password,
                                    database=rds_db_name, connect_timeout=5)
        cursor = connection.cursor()

        # Create table if it doesn't exist (replace with your table schema)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_data (
                store INT,
                dept INT,
                date DATE,
                weekly_sales FLOAT,
                isholiday BOOLEAN
            );
        """)

        # Insert data into RDS
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO sales_data (store, dept, date, weekly_sales, isholiday) VALUES (%s, %s, %s, %s, %s)
            """, (row['store'], row['dept'], row['date'], row['weekly_sales'], row['isholiday']))
        
        connection.commit()
    except Exception as e:
        print(f"Error connecting to RDS: {str(e)}")
        return {"statusCode": 500, "body": json.dumps("Failed to insert data into RDS")}
    finally:
        cursor.close()
        connection.close()

    return {"statusCode": 200, "body": json.dumps("Data loaded successfully into RDS")}

