#!/bin/bash

# $1 number of events
# $2 prefix for dataset directory

echo "Populating topics..."
for ((i=0; i<=50; i+=10)); do
    java -cp ../target/COSQA-jar-with-dependencies.jar linearroad.ProducingLinearRoad $1 $2 $i &> production-linearroad-inc-$i.out &
done
java -cp ../target/COSQA-jar-with-dependencies.jar reviews.ProducingReviewsSimulTime $1 $2 &> production-reviews.out &
java -cp ../target/COSQA-jar-with-dependencies.jar gps.ProducingGPS $1 $2 &> production-gps.out &
java -cp ../target/COSQA-jar-with-dependencies.jar stocks.ProducingStocksSimulTime $1 $2 &> production-stocks.out &

wait
echo "Population Ended."
