#!/bin/bash

# Configuration
GATEWAY_URL="http://localhost:3001/api/pay"
NUM_REQUESTS=10

echo "🚀 Starting Horizontal Scaling Test..."
echo "Sending $NUM_REQUESTS payment requests to $GATEWAY_URL"

for i in $(seq 1 $NUM_REQUESTS)
do
  echo "Sending request #$i..."
  curl -s -X POST $GATEWAY_URL \
    -H "Content-Type: application/json" \
    -d "{
      \"userId\": \"user_$i\",
      \"amount\": $((RANDOM % 1000 + 1)),
      \"receiverId\": \"receiver_$((RANDOM % 5 + 1))\"
    }" | jq .
  echo -e "\n"
done

echo "✅ All requests sent!"
echo "To verify scaling:"
echo "1. Run: docker-compose up --build --scale transaction-engine=3"
echo "2. Check logs: docker-compose logs -f transaction-engine"
echo "You should see different container IDs (transaction-engine-1, transaction-engine-2, etc.) processing different transactions."
