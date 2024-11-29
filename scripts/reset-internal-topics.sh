
KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/kafka_2.13-3.1.0"


# Get a list of topics and filter for "changelog" or "repartition"
topics_to_delete=$($KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -E "changelog|repartition")

# Loop through each topic and delete it
echo "$topics_to_delete" | while IFS= read -r topic; do
  echo "Deleting topic: $topic"
  $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$topic"
done
wait