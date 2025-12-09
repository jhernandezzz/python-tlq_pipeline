import boto3
import csv
import io
from datetime import datetime
from datetime import date

def lambda_handler(event, context):

    bucketname = event["bucketname"]
    filename = event["filename"]

    s3 = boto3.client("s3")

    # ---------- Metrics ----------
    row_count = 0
    total_revenue_sum = 0.0
    total_profit_sum = 0.0

    # ---------- Duplicate checker ----------
    seen_order_ids = set()

    # ---------- Output CSV builder ----------
    output = io.StringIO()
    writer = csv.writer(output)

    # Header (identical to Java version)
    writer.writerow([
        "Region","Country","Item Type","Sales Channel","Order Priority",
        "Order Date","Order ID","Ship Date","Units Sold","Unit Price",
        "Unit Cost","Total Revenue","Total Cost","Total Profit",
        "Order Processing Time","Gross Margin","Order Value"
    ])

    # ---------- Load input CSV ----------
    obj = s3.get_object(Bucket=bucketname, Key=filename)
    body = obj["Body"].read().decode("utf-8")
    reader = csv.reader(body.splitlines())

    # Skip header
    next(reader, None)

    # Date format
    date_format = "%m/%d/%Y"

    for row in reader:

        if len(row) < 14:
            # Skip malformed rows (same behavior)
            continue

        (
            region, country, itemType, salesChannel, orderPriorityRaw,
            orderDateStr, orderId, shipDateStr,
            unitsSold, unitPrice, unitCost,
            totalRevenue, totalCost, totalProfit
        ) = row

        # Deduplicate
        if orderId in seen_order_ids:
            continue
        seen_order_ids.add(orderId)

        # Priority mapping
        priority_map = {
            "L": "Low",
            "M": "Medium",
            "H": "High",
            "C": "Critical"
        }
        orderPriority = priority_map.get(orderPriorityRaw, orderPriorityRaw)

        # Convert numbers
        unitsSold = int(unitsSold)
        unitPrice = float(unitPrice)
        unitCost = float(unitCost)
        totalRevenue = float(totalRevenue)
        totalCost = float(totalCost)
        totalProfit = float(totalProfit)

        # Order processing time
        orderDate = datetime.strptime(orderDateStr, date_format).date()
        shipDate = datetime.strptime(shipDateStr, date_format).date()
        orderProcessingTime = (shipDate - orderDate).days

        # Gross margin
        grossMargin = totalProfit / totalRevenue

        # Order value
        orderValue = unitsSold * unitPrice

        # Write transformed row
        writer.writerow([
            region, country, itemType, salesChannel, orderPriority,
            orderDateStr, orderId, shipDateStr,
            unitsSold, unitPrice, unitCost,
            totalRevenue, totalCost, totalProfit,
            orderProcessingTime, grossMargin, orderValue
        ])

        # Metrics
        row_count += 1
        total_revenue_sum += totalRevenue
        total_profit_sum += totalProfit

    # ---------- Upload transformed CSV to S3 ----------
    timestamp = datetime.now().isoformat().replace(":", "-")
    output_key = f"transformed/{filename}_transformed_{timestamp}.csv"

    s3.put_object(
        Bucket=bucketname,
        Key=output_key,
        Body=output.getvalue(),
        ContentType="text/csv"
    )

    # ---------- Metrics ----------
    avg_revenue = 0 if row_count == 0 else total_revenue_sum / row_count
    avg_profit = 0 if row_count == 0 else total_profit_sum / row_count

    # ---------- Return JSON (matches Java version) ----------
    return {
        "value": f"Transformed {row_count} rows. AvgRevenue={avg_revenue} AvgProfit={avg_profit}",
        "rows_transformed": row_count,
        "avg_revenue": avg_revenue,
        "avg_profit": avg_profit,
        "output_key": output_key,
        "bucketname": bucketname
    }