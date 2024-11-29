# COSQA: Consistency-Aware Continuous Query Answering

This application allows to perform consistency-aware query answering through three implementations (inkstream/NCOSQAList, inkstream-optim/NCOSQAGraph, and naive/KCOSQA) realized on top of Kafka Streams. 
A fourth implementation without consistency awareness (noinc/NI) is included in the repo. 
A streaming-adapted, database Baseline implementation is located in [this](https://github.com/semlanghi/INCA) repository.

## Experiments 

### Qualitative (preliminary)

In the following, we report a qualitative study that we performed on an aggregate query that calculates the total cost of electric consumption, wrt the following query 

```
SELECT area,sum(consA)*1.2+sum(consB)*1.5,ts
FROM Consumption [RANGE 5 minutes SLIDE 2 minutes]
WHERE consA >= 0 AND consB >= 0
GROUP BY area;
```

We analysed 4 (+1) different approaches for dealing with inconsistencies derivated by two speed constraints applied over the stream, i.e., SC1 and SC2.
In our scenario, _we consider inconsistent the exclusive violation of either SC1 or SC2, but not the simultaneous violation of both_.
The 4 approaches are:

- `validation`: a database-inspired solutions where inconsistent records are simply filtered out of the stream, thus undersampling the aggregate
- `repair`: a solution derived from [1], where is performed a repair operation over streams with respect to Speed Constraints
- `groundtruth`: is the actual groundtruth
- `noinc`: no inconsistency management is performed
- `inkstream`: our approach

 

In our approach (`inkstream`), we readjusted the value based on the information contained in the polynomial, but we were also able to _ignore the simultaneous violation of SC1 and SC2_, resulting in a result that is nearer to the `groundtruth`.


![](qualitative_study.png)




### Performances

The experiments were performed using Java 17.0.2 and built through Maven 3.8.1. 
On top of this, it is necessary to download Apache Kafka 3.1.0.
Once everything is downloaded, startup Apache Kafka environment, first Zookeeper
```
KAFKA_DIR/bin/zookeeper-server-start.sh KAFKA_DIR/config/zookeeper.properties
```
then, the Kafka Broker (the default configuration is fine if the experiments are run locally).
```
KAFKA_DIR/bin/kafka-server-start.sh KAFKA_DIR/config/server.properties
```

Build the Maven project
```
cd cosqa
mvn clean
mvn install
mvn package
```



Run the following script to create and populate Kafka topics, where `nrecords` is the number of records inside each topic (datasets are replicated if the number exceeds the dataset max quantity of records), and `DATASET_DIR` is the local dataset directory, which can be downloaded from this [link](https://drive.google.com/drive/folders/153vr5Id4PTGR8Art0Ebf9wuEr_3jCSOj?usp=share_link).

```
.PROJECT_DIR/scripts/populate-topics.sh nrecords DATASET_DIR KAFKA_DIR
```



Run the following script to start the experiments, where `nrecords` are the number of records that each query needs to process in each experiment, and `RESULTS_DIR` is the directory where to put the results.

```
.PROJECT_DIR/scripts/cosqa-run-experiments.sh RESULTS_DIR nrecords_to_process granularity
```

This other script runs KCOSQA experiments
```
.PROJECT_DIR/scripts/kcosqa-process.sh RESULTS_DIR nrecords_to_process granularity
```

NOTE: `granularity` is a debug parameter, and it should always be bigger than `nrecords_to_process` when running the experiments. 

The baseline experiments can be executed through the `run-inca.sh` script, with the same argument as the previous two scripts (consider that the baseline is far slower, thus it is reasonable to use a reduced number of events).

In the following, the performance results in terms of throughput, memory consumption, time percentage with respect to annotation and consumption overhead, scalability with respect to the number of constraints used and number of inconsistencies detected, an ablation study based on which constraints are used in which experiments, and finally a comaprative study with the technique from [1], that we have directly imported from their [repository](https://github.com/apache/iotdb/blob/master/library-udf/src/main/java/org/apache/iotdb/library/drepair/util/Screen.java).

![](throughput.png)
![](memory.png)
![](annotatointime.png)
![](numberofconstraints.png)
<p align="center">
    <img src="./throughput_inc.png" width=50%>
</p>

![](throughputablation.png)
![](throughputcmp.png)


[1] Song, Shaoxu, et al. "SCREEN: stream data cleaning under speed constraints." Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data. 2015.


