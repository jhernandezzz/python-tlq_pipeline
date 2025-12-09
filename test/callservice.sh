#!/bin/bash

# Input parameters
bucketname="sales.data.tlq.jeh.2025"
filename="100 Sales Records.csv"

echo "=========================================="
echo "STEP 1: TransformCSV"
echo "=========================================="
json='{"bucketname":"'$bucketname'","filename":"'$filename'"}'
echo "Invoking TransformCSV Python via API Gateway"
time transform_output=$(curl -s -H "Content-Type: application/json" \
-X POST -d "$json" \
https://zbrdb66hk1.execute-api.us-east-2.amazonaws.com/prod/transform/python)

echo ""
echo "TransformCSV RESULT:"
echo "$transform_output" | jq
echo ""

# Extract output_key and bucketname from TransformCSV response
output_key=$(echo "$transform_output" | jq -r '.output_key')
returned_bucket=$(echo "$transform_output" | jq -r '.bucketname')

# Check if we got valid output
if [ "$output_key" == "null" ] || [ -z "$output_key" ]; then
    echo "ERROR: TransformCSV did not return output_key. Cannot proceed to LoadCSV."
    exit 1
fi

echo "Extracted output_key: $output_key"
echo ""

echo "=========================================="
echo "STEP 2: LoadCSV"
echo "=========================================="
load_json='{"bucketname":"'$returned_bucket'","key":"'$output_key'"}'
echo "Invoking LoadCSV Python via API Gateway"
time load_output=$(curl -s -H "Content-Type: application/json" \
-X POST -d "$load_json" \
https://i6qf6w3bob.execute-api.us-east-2.amazonaws.com/prod)

echo ""
echo "LoadCSV RESULT:"
echo "$load_output" | jq
echo ""

# Check if LoadCSV succeeded
status_code=$(echo "$load_output" | jq -r '.statusCode')
if [ "$status_code" != "200" ]; then
    echo "ERROR: LoadCSV failed. Cannot proceed to QueryDB."
    exit 1
fi

echo "=========================================="
echo "STEP 3: QueryDB"
echo "=========================================="

# Example query: Get total revenue and profit by region
query_json='{
  "filters": {},
  "groupBy": ["Region"],
  "aggregations": {
    "total_revenue": "SUM(Total Revenue)",
    "total_profit": "SUM(Total Profit)",
    "avg_order_value": "AVG(Total Revenue)",
    "order_count": "COUNT(order_id)"
  }
}'

echo "Invoking QueryDB Python via API Gateway"
echo "Query: Group by Region with revenue/profit aggregations"
time query_output=$(curl -s -H "Content-Type: application/json" \
-X POST -d "$query_json" \
https://w76l6gdngb.execute-api.us-east-2.amazonaws.com/prod)

echo ""
echo "QueryDB RESULT:"
echo "$query_output" | jq
echo ""

echo "=========================================="
echo "PIPELINE COMPLETE"
echo "=========================================="
echo "Summary:"
echo "- Transformed: $(echo "$transform_output" | jq -r '.rows_transformed') rows"
echo "- Loaded: $(echo "$load_output" | jq -r '.body.rows_inserted') rows"
echo "- Queried: $(echo "$query_output" | jq -r '.body.rows_returned') results"
echo "=========================================="