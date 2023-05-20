#!/bin/bash

#$1 : kafka directory
# Specify the Kafka broker URL and the prefixes of the topics you want to remove
KAFKA_BROKER="localhost:9092"
TOPIC_PREFIXES=("linearroad" "stocks" "gps" "reviews")

# Get the list of all topics
topics=$("$1"/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER)

# Iterate over the topics and delete those not meeting the specified conditions
for topic in $topics; do
    if [[ "$topic" != "__"* && "$topic" != "stocks" && "$topic" != "gps" && "$topic" != "linearroad" && "$topic" != "reviews" ]]; then
        echo "Deleting topic: $topic"
        "$1"/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$topic"
        # Add your desired logic or commands here
    fi
done