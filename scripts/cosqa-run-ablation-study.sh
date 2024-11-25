#!/bin/bash

#$1 RESULTS_DIR
#$2 NRECORDS_TO_PROCESS
#$3 GRANULARITY

MEMORY="15g"

for ((z=0; z<=1; z++)); do

  echo "Comparative Evaluation on Window Size/Slide of Only the Annotation Phase (only consumption through not aware pipeline)"
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
              done
          done
      done
  done
done









