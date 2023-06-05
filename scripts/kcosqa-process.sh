#!/bin/bash


#$1 RESULTS_DIR
#$2 NRECORDS_TO_PROCESS
#$3 GRANULARITY

MEMORY="15g"

for ((z=1; z<=1; z++)); do

  echo "Comparative Evaluation on Window Size/Slide"
  constraint_strictness=(1)
  window_size=(10 100 1000 10000)
  window_size_slide_factor=(2 5)


  name_experiment=("KCOSQA")

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
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockCombo "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "300000" "-1" "$topic" &> ${name_experiment[$i]}-stock-combo.out &
                      wait
                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearson "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$(($2))" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-pearson.out &
                      wait
                  done
              done
          done
      done
  done
#
#  echo "Detailed Evaluation on Constraint Strictness"
#  constraint_strictness=(5 10 20)
#  window_size=(1000 100)
#  window_size_slide_factor=(5)
#
#
#  name_experiment=("KCOSQA")
#
#  # Iterate over the array of strings
#  for ((i=0; i<${#name_experiment[@]}; i++)); do
#      echo "Starting Experiment: ${name_experiment[$i]}"
#      for cs in "${constraint_strictness[@]}"; do
#          echo "Current Constraint Strictness: $cs"
#          for ws in "${window_size[@]}"; do
#              echo "Current Window Size: $ws"
#              for wssf in "${window_size_slide_factor[@]}"; do
#                  echo "Current Size and Slide Factor: $wssf"
#                  for ((j=1; j<=1; j++)); do
#                      echo "Iteration $j"
#                      topic="reviews-nrecords-$3"
#                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Review "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$(($2/constraint_strictness))" $(($2/10)) "$topic" &> ${name_experiment[$i]}-review.out &
#                      wait
#                      topic="gps-nrecords-$3"
#                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"GPS "$cs" 0 "$ws" $((ws/wssf)) "$1" "$(($2/constraint_strictness))" $(($2/10)) "$topic" &> ${name_experiment[$i]}-gps.out &
#                      wait
#                      topic="stocks-nrecords-$3"
#                      cs2=$((constraint_strictness/5))
#                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockCombo "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$((300000/(cs2)))" "-1" "$topic" &> ${name_experiment[$i]}-stock-combo.out &
#                      wait
#                      java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearson "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$(($2/(cs2)))" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-pearson.out &
#                      wait
#                  done
#              done
#          done
#      done
#  done
#
#
#  echo "Detailed Evaluation on Inconsistency Percentage"
#  incons_perc=(10 20 30 40 50)
#  window_size=(1000 100)
#  window_size_slide_factor=(5)
#
#
#  name_experiment=("KCOSQA")
#
#  # Iterate over the array of strings
#  for ((i=0; i<${#name_experiment[@]}; i++)); do
#      echo "Starting Experiment: ${name_experiment[$i]}"
#      for cs in "${incons_perc[@]}"; do
#          echo "Current Inconsistency Percentage: $cs"
#          for ws in "${window_size[@]}"; do
#              echo "Current Window Size: $ws"
#              for wssf in "${window_size_slide_factor[@]}"; do
#                  echo "Current Size and Slide Factor: $wssf"
#                  for ((j=1; j<=1; j++)); do
#                      echo "Iteration $j"
#                      topic="linearroad-nrecords-$3"
#                      ip2=$((incons_perc/10))
#                      if [ $ip2 = 0 ]; then
#                          java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad 1 "$cs" "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$((70000/ip2))" "-1" "$topic" &> ${name_experiment[$i]}-linearroad.out &
#                          wait
#                      fi
#                      if [ $ip2 != 0 ]; then
#                        java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad 1 "$cs" "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$((70000/ip2))" "-1" "$topic" &> ${name_experiment[$i]}-linearroad.out &
#                        wait
#                      fi
#                  done
#              done
#          done
#      done
#  done

done









