import json
import pymysql
import logging
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Allowed aggregations to prevent SQL injection ---
ALLOWED_FUNCS = {'SUM', 'AVG', 'MIN', 'MAX', 'COUNT'}

# --- Map readable names to database column names ---
COLUMN_MAP = {
    "Region": "region",
    "Country": "country",
    "Item Type": "item_type",
    "Sales Channel": "sales_channel",
    "Order Priority": "order_priority",
    "Order Date": "order_date",
    "Ship Date": "ship_date",
    "Units Sold": "units_sold",
    "Unit Price": "unit_price",
    "Unit Cost": "unit_cost",
    "Total Revenue": "total_revenue",
    "Total Cost": "total_cost",
    "Total Profit": "total_profit",
    "Order Processing Time": "order_processing_time",
    "Gross Margin": "gross_margin"
}


def normalize_column(input_str):
    """Normalize column name from readable format to database column name."""
    return COLUMN_MAP.get(input_str, input_str.lower().replace(" ", "_"))


def lambda_handler(event, context):
    """
    Lambda handler to query the Aurora MySQL sales database with dynamic filters,
    group-by, and aggregations.

    Expected event structure:
    {
        "filters": {
            "Region": "Europe",
            "Item Type": "Fruits"
        },
        "groupBy": ["Country", "Sales Channel"],
        "aggregations": {
            "total_revenue": "SUM(Total Revenue)",
            "avg_profit": "AVG(Total Profit)",
            "order_count": "COUNT(Order ID)"
        }
    }
    """

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
            cursorclass=pymysql.cursors.DictCursor  # Return results as dictionaries
        )
        logger.info("Connected to DB")

        # ---------- 3. Parse Input ----------
        filters = event.get('filters', {})
        group_by = event.get('groupBy', [])
        aggregations = event.get('aggregations', {})

        # ---------- 4. SQL Construction ----------
        select_fields = []

        # Add aggregation fields
        for alias, expr in aggregations.items():
            expr = expr.strip()

            # Parse function and column: e.g., "SUM(Total Revenue)"
            if '(' not in expr or ')' not in expr:
                raise ValueError(f"Invalid aggregation format: {expr}")

            func = expr[:expr.index('(')].upper()

            if func not in ALLOWED_FUNCS:
                raise ValueError(f"Invalid aggregation function: {func}")

            # Extract column inside parentheses
            col = expr[expr.index('(') + 1:expr.index(')')].strip()
            col = normalize_column(col)

            select_fields.append(f"{func}({col}) AS {alias}")

        # Add group-by columns
        if group_by:
            for col_raw in group_by:
                col = normalize_column(col_raw)
                select_fields.append(col)

        # Build SELECT clause
        if not select_fields:
            raise ValueError("No fields to select. Provide aggregations or groupBy.")

        sql = "SELECT " + ", ".join(select_fields)
        sql += " FROM sales"

        # WHERE clause
        where_values = []
        if filters:
            where_clauses = []
            for key, value in filters.items():
                col = normalize_column(key)
                where_clauses.append(f"{col} = %s")
                where_values.append(value)

            sql += " WHERE " + " AND ".join(where_clauses)

        # GROUP BY clause
        if group_by:
            group_cols = [normalize_column(col_raw) for col_raw in group_by]
            sql += " GROUP BY " + ", ".join(group_cols)

        logger.info(f"Final SQL: {sql}")
        logger.info(f"Parameters: {where_values}")

        # ---------- 5. Execute query ----------
        with connection.cursor() as cursor:
            cursor.execute(sql, where_values)
            results = cursor.fetchall()

        logger.info(f"Rows returned: {len(results)}")

        # ---------- 6. Return results ----------
        return {
            'statusCode': 200,
            'body': {
                'query_sql': sql,
                'rows_returned': len(results),
                'results': results
            }
        }

    except Exception as e:
        logger.error(f"QueryDB ERROR: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        }

    finally:
        if connection:
            connection.close()