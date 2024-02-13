#!/bin/bash

#$1 : kafka directory
# Specify the Kafka broker URL and the prefixes of the topics you want to remove
KAFKA_BROKER="localhost:9092"
TOPIC_PREFIXES=("linearroad" "stocks" "gps" "reviews")

# Get the list of all topics
topics=$($1/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER)

# Loop through each topic prefix
for prefix in "${TOPIC_PREFIXES[@]}"; do
    # Filter topics that match the current prefix
    filtered_topics=$(echo "$topics" | grep "^$prefix")

    # Loop through each matching topic and delete it
    while read -r topic; do
        echo "Deleting topic: $topic"
        $1/bin/kafka-topics.sh --delete --bootstrap-server $KAFKA_BROKER --topic $topic
    done <<< "$filtered_topics"
done