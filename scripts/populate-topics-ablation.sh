#!/bin/bash

# $1 number of events
# $2 kafka dir



echo "Creating $1 electric-grid events"
$2/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "electricgrid-nrecords-$1" --replication-factor 1
java -cp ../target/COSQA-jar-with-dependencies.jar electricgrid.ProducingElectricGridAblation $1 &> production-electricgrid.out &
wait
echo "Population Ended."
