package reviews;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;


import java.io.File;
import java.time.Duration;
import java.util.Properties;

public class ProducingReviewsSimulTime {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ReviewSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        long nrecords = Long.parseLong(args[0]);
        String dirPrefix = args[1];
        String topic = "reviews-nrecords-" + (nrecords == -1 ? "total" : nrecords);


        KafkaProducer<String, Review> producer = new KafkaProducer<>(props);

        try {

            File dir = new File(dirPrefix+"/reviews/");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {

                //Initialize vector of splitters
                ReviewPreprocessing[] splitters = new ReviewPreprocessing[directoryListing.length];
                for (int j=0; j < directoryListing.length; j++) {
                    splitters[j] = new ReviewPreprocessing(directoryListing[j].getAbsolutePath());
                }




                if (nrecords > -1){
//                    long[] startTimestamp = new long[splitters.length];
//                    long[] lastRegisteredTimestamp = new long[splitters.length];
//                    for (int i = 0; i < splitters.length; i++) {
//                        startTimestamp[i] = 0L;
//                        lastRegisteredTimestamp[i] = 0L;
//                    }
                    nrecords = nrecords / splitters.length;
                    //Sending records with simulated timestamp
                    for (long i = 0; i < nrecords; i++) {
                        for (int j=0; j < directoryListing.length; j++) {
                            if (splitters[j] != null){
                                Review read = splitters[j].read(Duration.ofMillis(1000).toMillis()*i);

                                if (read!=null) {
//                                    lastRegisteredTimestamp[j] = read.timestamp();
//                                    read.addTimestamp(startTimestamp[j]);
                                    producer.send(new ProducerRecord<>(topic, read.title, read));
                                } else {
//                                    startTimestamp[j] += lastRegisteredTimestamp[j];
                                    splitters[j].close();
                                    splitters[j] = new ReviewPreprocessing(directoryListing[j].getAbsolutePath());
                                }
                            }
                        }
                    }

                    //Closing the splitters
                    for (int j=0; j < directoryListing.length; j++) {
                        splitters[j].close();
                    }
                } else {
                    //Sending records with simulated timestamp, the whole file, no reset
                    long i = 0L;
                    boolean terminated = false;
                    while (!terminated) {
                        terminated = true;
                        for (int j=0; j < directoryListing.length; j++) {
                            if (splitters[j] != null){
                                terminated = false;
                                Review read = splitters[j].read(Duration.ofMillis(1000).toMillis()*i);
                                if (read!=null) {
                                    producer.send(new ProducerRecord<>("sample-topic", read.title, read));
                                } else {
                                    splitters[j].close();
                                    splitters[j] = null;
                                }
                            }
                        }
                        i++;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
