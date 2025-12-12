import json
import boto3
import pymysql
import csv
from io import StringIO
import logging
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Lambda handler to load transformed CSV from S3 into Aurora MySQL.

    UPDATED: Now uses TRUNCATE to overwrite old data and batch inserts for performance.

    Expected event structure:
    {
        "bucketname": "your-bucket-name",
        "key": "path/to/transformed.csv"
    }
    """

    # Initialize response
    response = {
        'statusCode': 200,
        'body': {}
    }

    # Extract parameters from event
    bucket = event.get('bucketname')
    key = event.get('key')

    logger.info(f"LoadCSV invoked. bucket={bucket} key={key}")

    rows_read = 0
    rows_inserted = 0
    connection = None

    try:
        # ---------- 1. Load DB properties from environment variables ----------
        db_host = os.environ.get('DB_HOST')
        db_user = os.environ.get('DB_USER')
        db_password = os.environ.get('DB_PASSWORD')
        db_name = os.environ.get('DB_NAME', 'SALES')

        if not db_host or not db_user or not db_password:
            raise ValueError("Missing required environment variables: DB_HOST, DB_USER, or DB_PASSWORD")

        # ---------- 2. Connect to Aurora MySQL ----------
        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            connect_timeout=5,
            autocommit=False  # Manual transaction control for performance
        )
        logger.info(f"Connected to Aurora MySQL at {db_host}")

        # ---------- 3. Ensure SALES.sales table exists ----------
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS sales (
            order_id BIGINT PRIMARY KEY,
            region VARCHAR(64),
            country VARCHAR(64),
            item_type VARCHAR(64),
            sales_channel VARCHAR(32),
            order_priority VARCHAR(32),
            order_date VARCHAR(32),
            ship_date VARCHAR(32),
            units_sold INT,
            unit_price DOUBLE,
            unit_cost DOUBLE,
            total_revenue DOUBLE,
            total_cost DOUBLE,
            total_profit DOUBLE,
            order_processing_time INT,
            gross_margin DOUBLE
        )
        """

        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            connection.commit()

        logger.info("Ensured table SALES.sales exists.")

        # ---------- 3b. OVERWRITE OLD DATA ----------
        # Clear old rows so DB matches the current CSV exactly
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE sales")
            connection.commit()
            logger.info("TRUNCATE TABLE sales executed. Old data removed.")

        # ---------- 4. Prepare insert statement ----------
        # Simple INSERT (no IGNORE) because duplicates within the file should not exist
        insert_sql = """
        INSERT INTO sales (
            order_id, region, country, item_type, sales_channel, order_priority,
            order_date, ship_date, units_sold, unit_price, unit_cost,
            total_revenue, total_cost, total_profit, order_processing_time, gross_margin
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        # ---------- 5. Read transformed CSV from S3 ----------
        s3_client = boto3.client('s3')
        logger.info(f"Fetching CSV from S3...")
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = s3_object['Body'].read().decode('utf-8')
        logger.info(f"CSV fetched. Size: {len(csv_content)} bytes")

        # ---------- 6. Parse CSV and map column indices ----------
        csv_reader = csv.DictReader(StringIO(csv_content))

        # Expected column names (must match TransformCSV output)
        required_cols = [
            "Region", "Country", "Item Type", "Sales Channel", "Order Priority",
            "Order Date", "Order ID", "Ship Date", "Units Sold", "Unit Price",
            "Unit Cost", "Total Revenue", "Total Cost", "Total Profit",
            "Order Processing Time", "Gross Margin"
        ]

        # Verify headers
        fieldnames = csv_reader.fieldnames
        for col in required_cols:
            if col not in fieldnames:
                logger.warning(f"WARNING: header '{col}' not found in CSV. Check TransformCSV output.")

        logger.info("Starting to process rows with batch inserts...")

        # ---------- 7. Read and insert each row (BATCHED) ----------
        BATCH_SIZE = 1000
        batch = []

        with connection.cursor() as cursor:
            for row in csv_reader:
                if not row or all(v.strip() == '' for v in row.values()):
                    continue

                rows_read += 1

                try:
                    # Extract values with fallback to None
                    order_id_str = row.get('Order ID', '').strip()

                    if not order_id_str:
                        logger.info(f"Skipping row with missing Order ID: {row}")
                        continue

                    # Parse values
                    order_id = int(order_id_str)
                    region = row.get('Region', '').strip()
                    country = row.get('Country', '').strip()
                    item_type = row.get('Item Type', '').strip()
                    sales_channel = row.get('Sales Channel', '').strip()
                    order_priority = row.get('Order Priority', '').strip()
                    order_date = row.get('Order Date', '').strip()
                    ship_date = row.get('Ship Date', '').strip()
                    units_sold = int(row.get('Units Sold', '0'))
                    unit_price = float(row.get('Unit Price', '0'))
                    unit_cost = float(row.get('Unit Cost', '0'))
                    total_revenue = float(row.get('Total Revenue', '0'))
                    total_cost = float(row.get('Total Cost', '0'))
                    total_profit = float(row.get('Total Profit', '0'))
                    order_proc_time = int(row.get('Order Processing Time', '0'))
                    gross_margin = float(row.get('Gross Margin', '0'))

                    # Add to batch
                    batch.append((
                        order_id, region, country, item_type, sales_channel, order_priority,
                        order_date, ship_date, units_sold, unit_price, unit_cost,
                        total_revenue, total_cost, total_profit, order_proc_time, gross_margin
                    ))

                    # Execute batch when it reaches BATCH_SIZE
                    if len(batch) >= BATCH_SIZE:
                        cursor.executemany(insert_sql, batch)
                        connection.commit()
                        rows_inserted += len(batch)
                        logger.info(f"Inserted batch of {len(batch)} rows. Total: {rows_inserted}")
                        batch = []

                except Exception as row_ex:
                    logger.error(f"Error parsing/inserting row: {row}")
                    logger.error(f"Row exception: {str(row_ex)}")

            # Execute any remaining batch
            if batch:
                cursor.executemany(insert_sql, batch)
                connection.commit()
                rows_inserted += len(batch)
                logger.info(f"Inserted final batch of {len(batch)} rows. Total: {rows_inserted}")

        summary = f"LoadCSV complete. rowsRead={rows_read}, rowsInserted={rows_inserted}"
        logger.info(summary)

        response['body'] = {
            'message': summary,
            'rows_read': rows_read,
            'rows_inserted': rows_inserted
        }

    except Exception as e:
        # If anything goes wrong, try to roll back the transaction
        if connection:
            try:
                connection.rollback()
                logger.info("Transaction rolled back due to error")
            except Exception:
                pass

        logger.error(f"LoadCSV ERROR: {str(e)}")
        response['statusCode'] = 500
        response['body'] = {
            'message': f"Load failed: {str(e)}"
        }

    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass

    return response