#!/bin/bash

#$1 RESULTS_DIR
#$2 NRECORDS_TO_PROCESS
#$3 GRANULARITY
KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/kafka_2.13-3.1.0"


MEMORY="15g"

for ((z=0; z<=2; z++)); do

  echo "Evaluation on Window Size/Slide of the whole query"
  constraint_strictness=(1)
  window_size=(10 100 1000 10000)
  window_size_slide_factor=(2 5)


  name_experiment=("NCOSQAGraph" "NCOSQAPKGraph" "NCOSQAList" "NCOSQAPKList" "NCOSQASchemaConstraint" "NI")

  # Iterate over the array of strings
  for ((i=0; i<${#name_experiment[@]}; i++)); do
      echo "Starting Experiment: ${name_experiment[$i]}"
      for cs in "${constraint_strictness[@]}"; do
          echo "Current Constraint Strictness: $cs"
          for ws in "${window_size[@]}"; do
              echo "Current Window Size: $ws"
              for wssf in "${window_size_slide_factor[@]}"; do
                  echo "Current Size and Slide Factor: $wssf"
                  for ((j=1; j<=1; j++)); do
                      echo "Iteration $j"
                      topic="reviews-nrecords-$3"
                      echo java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Review "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" $(($2*2)) "$topic"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Review "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-review.out &
                      wait
                      topic="gps-nrecords-$3"
                      echo java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"GPS "$cs" 0 "$ws" $((ws/wssf)) "$1" "$2" $(($2*2)) "$topic"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"GPS "$cs" 0 "$ws" $((ws/wssf)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-gps.out &
                      wait
                      topic="linearroad-nrecords-$3"
                      echo java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "70000" "-1" "$topic"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "70000" "-1" "$topic" &> ${name_experiment[$i]}-linearroad.out &
                      wait
                      topic="stocks-nrecords-$3"
                      echo java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockCombo "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" "-1" "$topic"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockCombo "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" "-1" "$topic" &> ${name_experiment[$i]}-stock-combo.out &
                      wait
                      echo java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearson "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$(($2))" $(($2*2)) "$topic"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearson "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$(($2))" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-pearson.out &
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


  echo "Evaluation on Window Size/Slide of Only the Annotation Phase (only consumption through not aware pipeline)"
  echo "Used for both runtime analysis and comparative analysis with Repair Technique"
  constraint_strictness=(1)
  window_size=(10 100 1000 10000)
  window_size_slide_factor=(2 5)


  name_experiment=("DummyNCOSQAGraph" "DummyRepair" "DummyNI" "DummyNCOSQAList")

  # Iterate over the array of strings
  for ((i=0; i<${#name_experiment[@]}; i++)); do
      echo "Starting Experiment: ${name_experiment[$i]}"
      for cs in "${constraint_strictness[@]}"; do
          echo "Current Constraint Strictness: $cs"
          for ws in "${window_size[@]}"; do
              echo "Current Window Size: $ws"
              for wssf in "${window_size_slide_factor[@]}"; do
                  echo "Current Size and Slide Factor: $wssf"
                  for ((j=1; j<=1; j++)); do
                      echo "Iteration $j"
                      topic="reviews-nrecords-$3"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Review "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-review-annot.out &
                      wait
                      topic="gps-nrecords-$3"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"GPS "$cs" 0 "$ws" $((ws/wssf)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-gps-annot.out &
                      wait
                      topic="linearroad-nrecords-$3-incons-0"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "70000" "-1" "$topic" &> ${name_experiment[$i]}-linearroad-annot.out &
                      wait
                      topic="stocks-nrecords-$3"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Stock "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" "-1" "$topic" &> ${name_experiment[$i]}-stock-annot.out &
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


  echo "Comparative Evaluation on Window Size/Slide With various number of violations"
  constraint_strictness=(10 20)
  inconsistency_percentage=(20 40 60 80 100)
  window_size=(1000)
  window_size_slide_factor=(5)


  name_experiment=("NCOSQAGraph" "NCOSQAList")

  # Iterate over the array of strings
  for ((i=0; i<${#name_experiment[@]}; i++)); do
      echo "Starting Experiment: ${name_experiment[$i]}"
      for cs in "${constraint_strictness[@]}"; do
        for ip in "${inconsistency_percentage[@]}"; do
            echo "Current Constraint Strictness: $cs"
            for ws in "${window_size[@]}"; do
                echo "Current Window Size: $ws"
                for wssf in "${window_size_slide_factor[@]}"; do
                    echo "Current Size and Slide Factor: $wssf"
                    for ((j=1; j<=1; j++)); do
                        echo "Iteration $j"
                        topic="reviews-nrecords-$3"
                        java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"ReviewNumberOfConstraints "$cs" "$ip" "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-review-ncons.out &
                        wait
                        topic="gps-nrecords-$3"
                        java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"GPSNumberOfConstraints "$cs" "$ip" "$ws" $((ws/wssf)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-gps-ncons.out &
                        wait
                        topic="stocks-nrecords-$3"
                        java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockComboNumberOfConstraints "$cs" "$ip" "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" "-1" "$topic" &> ${name_experiment[$i]}-stock-combo-ncons.out &
                        wait
                        java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearsonNumberOfConstraints "$cs" "$ip" "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$(($2))" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-pearson-ncons.out &
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
  done



done












