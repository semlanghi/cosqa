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


echo "Comparative Evaluation on Window Size/Slide"
constraint_strictness=(1)
window_size=(100 500 1000)
window_size_slide_factor=(1 2 5 10)


name_experiment=("NCOSQAGraph" "KCOSQA" "NI")

# Iterate over the array of strings
for ((i=0; i<${#name_experiment[@]}; i++)); do
    echo "Starting Experiment: ${name_experiment[$i]}"
    for cs in "${constraint_strictness[@]}"; do
        echo "Current Constraint Strictness: $cs"
        for ws in "${window_size[@]}"; do
            echo "Current Window Size: $ws"
            for wssf in "${window_size_slide_factor[@]}"; do
                echo "Current Size and Slide Factor: $wssf"
                for ((j=1; j<=3; j++)); do
                    echo "Iteration $j"
                    topic="reviews-nrecords-$3"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Review "$cs" 0 "$((ws*86400000))" $(((ws/wssf)*86400000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-review.out &
                    wait
                    topic="gps-nrecords-$3"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"GPS "$cs" 0 "$ws" $((ws/wssf)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-gps.out &
                    wait
                    topic="stocks-nrecords-$3"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearson "$cs" 0 "$((ws*86400000))" $(((ws/wssf)*86400000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-pearson.out &
                    wait
                    topic="stocks-nrecords-$3"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockCombo "$cs" 0 "$((ws*86400000))" $(((ws/wssf)*86400000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-combo.out &
                    wait
                    topic="linearroad-nrecords-$3-incons-0"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad "$cs" 0 "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-linearroad.out &
                    wait
                done
            done
        done
    done
done

echo "Detailed Evaluation on Constraint Strictness"
constraint_strictness=(1 10 100 1000 10000)
window_size=(500)
window_size_slide_factor=(5)


name_experiment=("NCOSQAGraph")

# Iterate over the array of strings
for ((i=0; i<${#name_experiment[@]}; i++)); do
    echo "Starting Experiment: ${name_experiment[$i]}"
    for cs in "${constraint_strictness[@]}"; do
        echo "Current Constraint Strictness: $cs"
        for ws in "${window_size[@]}"; do
            echo "Current Window Size: $ws"
            for wssf in "${window_size_slide_factor[@]}"; do
                echo "Current Size and Slide Factor: $wssf"
                for ((j=1; j<=3; j++)); do
                    echo "Iteration $j"
                    topic="reviews-nrecords-$3"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"Review "$cs" 0 "$((ws*86400000))" $(((ws/wssf)*86400000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-review.out &
                    wait
                    topic="gps-nrecords-$3"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"GPS "$cs" 0 "$ws" $((ws/wssf)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-gps.out &
                    wait
                    topic="stocks-nrecords-$3"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockPearson "$cs" 0 "$((ws*86400000))" $(((ws/wssf)*86400000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-pearson.out &
                    wait
                    topic="stocks-nrecords-$3"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"StockCombo "$cs" 0 "$((ws*86400000))" $(((ws/wssf)*86400000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-stock-combo.out &
                    wait
                done
            done
        done
    done
done


echo "Detailed Evaluation on Inconsistency Percentage"
incons_perc=(0 10 20 30 40 50)
window_size=(500)
window_size_slide_factor=(5)


name_experiment=("NCOSQAGraph")

# Iterate over the array of strings
for ((i=0; i<${#name_experiment[@]}; i++)); do
    echo "Starting Experiment: ${name_experiment[$i]}"
    for cs in "${incons_perc[@]}"; do
        echo "Current Inconsistency Percentage: $cs"
        for ws in "${window_size[@]}"; do
            echo "Current Window Size: $ws"
            for wssf in "${window_size_slide_factor[@]}"; do
                echo "Current Size and Slide Factor: $wssf"
                for ((j=1; j<=3; j++)); do
                    echo "Iteration $j"
                    topic="linearroad-nrecords-$3-incons-$cs"
                    java -Xmx"$MEMORY" -cp ../target/COSQA-jar-with-dependencies.jar "${name_experiment[$i]}"LinearRoad 1 "$cs" "$((ws*1000))" $(((ws/wssf)*1000)) "$1" "$2" $(($2*2)) "$topic" &> ${name_experiment[$i]}-linearroad.out &
                    wait
                done
            done
        done
    done
done









