#!/bin/bash
# debug-kafka-messages.sh
# Check what's actually in Kafka topics

echo "========================================"
echo "   KAFKA MESSAGE DEBUG TOOL"
echo "========================================"

echo ""
echo "1. Checking weather.wind topic..."
echo "----------------------------------------"
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather.wind \
  --from-beginning \
  --max-messages 3 \
  --property print.key=true \
  --property print.timestamp=true

echo ""
echo "2. Checking weather.sunshine topic..."
echo "----------------------------------------"
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather.sunshine \
  --from-beginning \
  --max-messages 3 \
  --property print.key=true \
  --property print.timestamp=true

echo ""
echo "3. Checking weather.aggregated.output topic..."
echo "----------------------------------------"
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather.aggregated.output \
  --from-beginning \
  --max-messages 3 \
  --property print.key=true \
  --property print.timestamp=true

echo ""
echo "4. Topic Details..."
echo "----------------------------------------"
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic weather.wind

echo ""
echo "========================================"
echo "   DEBUG COMPLETE"
echo "========================================"