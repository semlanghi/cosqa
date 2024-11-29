#!/bin/bash

# $1 number of events
# $2 prefix for dataset directory
# $3 kafka dir



echo "Populating topics..."
for ((i=0; i<=50; i+=10)); do
  echo "Creating $1 linearroad events with inconsistency $i"
  $3/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "linearroad-nrecords-$1-incons-$i" --replication-factor 1
  java -cp ../target/COSQA-jar-with-dependencies.jar linearroad.ProducingLinearRoad $1 $2 $i &> production-linearroad-inc-$i.out &
  wait
done
echo "Creating $1 reviews events"
$3/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "reviews-nrecords-$1" --replication-factor 1
java -cp ../target/COSQA-jar-with-dependencies.jar reviews.ProducingReviewsSimulTime $1 $2 &> production-reviews.out &
wait
echo "Creating $1 reviews dirty events"
$3/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "reviews-dirty-nrecords-$1" --replication-factor 1
echo "-cp ../target/COSQA-jar-with-dependencies.jar reviews.ProducingReviewsSimulTimeDirty $1 $2 &> production-reviews-dirty.out &"
java -cp ../target/COSQA-jar-with-dependencies.jar reviews.ProducingReviewsSimulTimeDirty $1 $2 &> production-reviews-dirty.out &
wait
echo "Creating $1 gps events"
$3/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "gps-nrecords-$1" --replication-factor 1
java -cp ../target/COSQA-jar-with-dependencies.jar gps.ProducingGPS $1 $2 &> production-gps.out &
wait
echo "Creating $1 gps dirty events"
$3/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "gps-dirty-nrecords-$1" --replication-factor 1
java -cp ../target/COSQA-jar-with-dependencies.jar gps.ProducingGPSDirty $1 $2 &> production-gps-dirty.out &
wait
echo "Creating $1 stocks events"
$3/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "stocks-nrecords-$1" --replication-factor 1
java -cp ../target/COSQA-jar-with-dependencies.jar stocks.ProducingStocksSimulTime $1 $2 &> production-stocks.out &
wait
echo "Creating $1 stocks dirty events"
$3/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "stocks-dirty-nrecords-$1" --replication-factor 1
java -cp ../target/COSQA-jar-with-dependencies.jar stocks.ProducingStocksSimulTimeDirty $1 $2 &> production-stocks-dirty.out &
wait
echo "Population Ended."
echo "Creating $1 electric-grid events"
$3/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic "electricgrid-nrecords-$1" --replication-factor 1
java -cp ../target/COSQA-jar-with-dependencies.jar electricgrid.ProducingElectricGridAblation $1 &> production-electricgrid.out &
wait
echo "Population Ended."
