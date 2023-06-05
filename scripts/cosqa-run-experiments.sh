#!/bin/bash

#$1 CONSTRAINT_STRICTNESS
#$2 INCONSISTENCY_PERCENTAGE, the percentage of inconsistency injected, >0 only for the linearroad case
#$3 WINDOW_SIZE_MS
#$4 WINDOW_SLIDE_MS
#$5 RESULT_FILE_DIR -- First argument of the script
#$6 EVENTS_MAX is the number of events to consume -- Second Argument of the script
#$7 EVENTS_GRANULARITY Every how many events register performance
#$8 Events in the topic -- Third Argument of the script

MEMORY="15g"

for ((z=1; z<=2; z++)); do


  echo "Comparative Evaluation on Window Size/Slide"
  constraint_strictness=(1)
  window_size=(10 100 1000 10000)
  window_size_slide_factor=(2 5)


  name_experiment=("NCOSQAGraph" "NCOSQAList" "NI")

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
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Review "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-review.out &
                      wait
                      topic="gps-nrecords-$3"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"GPS "$cs" 0 "$ws" $((ws/wssf)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-gps.out &
                      wait
                      topic="linearroad-nrecords-$3"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "70000" "-1" "$topic" &> ${name_experiment[$i]}-linearroad.out &
                      wait
                      topic="stocks-nrecords-$3"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockCombo "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" "-1" "$topic" &> ${name_experiment[$i]}-stock-combo.out &
                      wait
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearson "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$(($2))" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-pearson.out &
                      wait
                  done
              done
          done
      done
  done

  echo "Comparative Evaluation on Window Size/Slide of Only the Annotation Phase (only consumption through not aware pipeline)"
  constraint_strictness=(1)
  window_size=(10 100 1000 10000)
  window_size_slide_factor=(2 5)


  name_experiment=("DummyNCOSQAGraph" "DummyNCOSQAList" "DummyNI")

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
                      topic="linearroad-nrecords-$3"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "70000" "-1" "$topic" &> ${name_experiment[$i]}-linearroad-annot.out &
                      wait
                      topic="stocks-nrecords-$3"
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Stock "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" "-1" "$topic" &> ${name_experiment[$i]}-stock-annot.out &
                      wait
                  done
              done
          done
      done
  done


  echo "Comparative Evaluation on Window Size/Slide With various number of constraints"
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
                        topic="linearroad-nrecords-$3"
                        java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoadNumberOfConstraints "1" "$ip" "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" "-1" "$topic" &> ${name_experiment[$i]}-linearroad-ncons.out &
                        wait
                        topic="stocks-nrecords-$3"
                        java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockComboNumberOfConstraints "$cs" "$ip" "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" "-1" "$topic" &> ${name_experiment[$i]}-stock-combo-ncons.out &
                        wait
                        java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearsonNumberOfConstraints "$cs" "$ip" "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$(($2))" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-pearson-ncons.out &
                        wait
                    done
                done
            done
        done
      done
  done

done









