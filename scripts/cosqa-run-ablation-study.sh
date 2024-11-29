#!/bin/bash

#$1 RESULTS_DIR
#$2 NRECORDS_TO_PROCESS
#$3 GRANULARITY
#$4 KAFKA_HOME

MEMORY="15g"
KAFKA_HOME="$4"

for ((z=0; z<=2; z++)); do

  echo "Ablation Study on Window Size/Slide of the constraints used, Primary Key (PK), Speed (SC) and Schema (Sch) Constraints"
  constraint_considered=("SC" "PK" "Sch" "SC,PK" "SC,Sch" "PK,Sch" "SC,PK,Sch" "NI")
  window_size=(10 100 1000 10000)
  window_size_slide_factor=(2 5)

  # Iterate over the array of strings
  for cc in "${constraint_considered[@]}"; do
      echo "Current Constraint Strictness: $cc"
      for ws in "${window_size[@]}"; do
          echo "Current Window Size: $ws"
          for wssf in "${window_size_slide_factor[@]}"; do
              echo "Current Size and Slide Factor: $wssf"
              for ((j=1; j<=1; j++)); do
                  echo "Iteration $j"
                  topic="electricgrid-nrecords-$3"
                  if [ "$cc" == "NI" ]; then
                      echo java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar electricgrid.ExampleAblationSCPKSchNI "25" 0 "$((ws))" $(((ws/wssf))) "$1" "$2" $(($2*2)) "$topic" "$cc"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar electricgrid.ExampleAblationSCPKSchNI "25" 0 "$((ws))" $(((ws/wssf))) "$1" "$2" $(($2*2)) "$topic" "$cc" &> egc-ablation-"$cc".out &
                  else
                      echo java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar electricgrid.ExampleAblationSCPKSch "25" 0 "$((ws))" $(((ws/wssf))) "$1" "$2" $(($2*2)) "$topic" "$cc"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar electricgrid.ExampleAblationSCPKSch "25" 0 "$((ws))" $(((ws/wssf))) "$1" "$2" $(($2*2)) "$topic" "$cc" &> egc-ablation-"$cc".out &
                  fi
                  wait

                  # Get a list of topics and filter for "changelog" or "repartition"
                  topics_to_delete=$($KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -E "changelog|repartition")

                  # Loop through each topic and delete it
                  echo "$topics_to_delete" | while IFS= read -r topic; do
                    echo "Deleting topic: $topic"
                    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$topic"
                  done
                  wait
              done
          done
      done
  done
done









