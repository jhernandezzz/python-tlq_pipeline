#!/bin/bash

# Input parameters
bucketname="sales.data.tlq.jeh.2025"
filename="100 Sales Records.csv"

###############################################
# Helper: measure time in seconds (float)
###############################################
now() { date +%s.%N; }

echo "=========================================="
echo "STEP 1: TransformCSV"
echo "=========================================="

json='{"bucketname":"'$bucketname'","filename":"'$filename'"}'
echo "Invoking TransformCSV Python via API Gateway"

start_transform=$(now)
transform_output=$(curl -w "\n%{time_total}" -s -H "Content-Type: application/json" \
  -X POST -d "$json" \
  https://zbrdb66hk1.execute-api.us-east-2.amazonaws.com/prod/transform/python)

# Split JSON and curl timing
transform_json=$(echo "$transform_output" | head -n -1)
transform_latency=$(echo "$transform_output" | tail -n 1)
end_transform=$(now)

duration_transform=$(echo "$end_transform - $start_transform" | bc)

echo ""
echo "TransformCSV RESULT:"
echo "$transform_json" | jq
echo ""

# Extract output key
output_key=$(echo "$transform_json" | jq -r '.output_key')
returned_bucket=$(echo "$transform_json" | jq -r '.bucketname')
rows_transformed=$(echo "$transform_json" | jq -r '.rows_transformed')

if [ "$output_key" == "null" ] || [ -z "$output_key" ]; then
    echo "ERROR: TransformCSV did not return output_key. Cannot proceed."
    exit 1
fi

#########################################
# Display transform metrics
#########################################
echo "---- TransformCSV Metrics ----"
echo "Runtime (seconds):       $duration_transform"
echo "Network latency (s):     $transform_latency"
if [ "$rows_transformed" != "null" ] && [ "$rows_transformed" -gt 0 ]; then
  throughput_transform=$(echo "$rows_transformed / $duration_transform" | bc -l)
  echo "Throughput (rows/sec):   $throughput_transform"
else
  echo "Throughput:              N/A"
fi
echo ""


echo "=========================================="
echo "STEP 2: LoadCSV"
echo "=========================================="

load_json='{"bucketname":"'$returned_bucket'","key":"'$output_key'"}'
echo "Invoking LoadCSV Python via API Gateway"

start_load=$(now)
load_output=$(curl -w "\n%{time_total}" -s -H "Content-Type: application/json" \
  -X POST -d "$load_json" \
  https://i6qf6w3bob.execute-api.us-east-2.amazonaws.com/prod)

load_json_body=$(echo "$load_output" | head -n -1)
load_latency=$(echo "$load_output" | tail -n 1)
end_load=$(now)

duration_load=$(echo "$end_load - $start_load" | bc)
status_code=$(echo "$load_json_body" | jq -r '.statusCode')
rows_loaded=$(echo "$load_json_body" | jq -r '.body.rows_inserted')

echo ""
echo "LoadCSV RESULT:"
echo "$load_json_body" | jq
echo ""

if [ "$status_code" != "200" ]; then
    echo "ERROR: LoadCSV failed."
    exit 1
fi

#########################################
# Display load metrics
#########################################
echo "---- LoadCSV Metrics ----"
echo "Runtime (seconds):       $duration_load"
echo "Network latency (s):     $load_latency"
if [ "$rows_loaded" != "null" ] && [ "$rows_loaded" -gt 0 ]; then
  throughput_load=$(echo "$rows_loaded / $duration_load" | bc -l)
  echo "Throughput (rows/sec):   $throughput_load"
else
  echo "Throughput:              N/A"
fi
echo ""


echo "=========================================="
echo "STEP 3: QueryDB"
echo "=========================================="

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

start_query=$(now)
query_output=$(curl -w "\n%{time_total}" -s -H "Content-Type: application/json" \
  -X POST -d "$query_json" \
  https://w76l6gdngb.execute-api.us-east-2.amazonaws.com/prod)

query_json_body=$(echo "$query_output" | head -n -1)
query_latency=$(echo "$query_output" | tail -n 1)
end_query=$(now)

duration_query=$(echo "$end_query - $start_query" | bc)
rows_returned=$(echo "$query_json_body" | jq -r '.body.rows_returned')

echo ""
echo "QueryDB RESULT:"
echo "$query_json_body" | jq
echo ""

#########################################
# Display query metrics
#########################################
echo "---- QueryDB Metrics ----"
echo "Runtime (seconds):       $duration_query"
echo "Network latency (s):     $query_latency"

if [ "$rows_returned" != "null" ] && [ "$rows_returned" -gt 0 ]; then
  throughput_query=$(echo "$rows_returned / $duration_query" | bc -l)
  echo "Throughput (rows/sec):   $throughput_query"
else
  echo "Throughput:              N/A"
fi
echo ""


#########################################
# Final summary
#########################################
echo "=========================================="
echo "PIPELINE COMPLETE"
echo "=========================================="
echo "Summary:"
echo "- Transform runtime:  $duration_transform s"
echo "- Load runtime:       $duration_load s"
echo "- Query runtime:      $duration_query s"
echo "- Total runtime:      $(echo "$duration_transform + $duration_load + $duration_query" | bc) s"
echo ""
echo "- Transformed rows:   $rows_transformed"
echo "- Loaded rows:        $rows_loaded"
echo "- Query results:      $rows_returned"
echo "=========================================="
