package linearroad;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

public class ProducingLinearRoadSimulTime {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.Long().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpeedEventSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        long nrecords = Long.parseLong(args[0]);
        int incons = Integer.parseInt(args[1]);
        long timeDiffMs = Long.parseLong(args[2]);
        String topic = "linearroad-sim-nrecords-" + nrecords + "-incons-" + incons + "-timediff-" + timeDiffMs;
        KafkaProducer<Long, SpeedEvent> producer = new KafkaProducer<>(props);

        try {

            File dir = new File("/Users/samuelelanghi/Documents/projects/cosqa/src/main/resources/linearroad/");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {

                //Initialize preprocessor, only one file in the folder, index 0
                LinearRoadPreprocessing splitter = incons == 0 ? new LinearRoadPreprocessing(directoryListing[0].getAbsolutePath())
                        : new LinearRoadPreprocessing(directoryListing[0].getAbsolutePath(), incons);

                boolean finished = false;
                if (nrecords>-1)
                    //Sending records with simulated timestamp
                    for (long i = 0; i < nrecords; i++) {
                        SpeedEvent read = splitter.read(Duration.ofMillis(timeDiffMs).toMillis()*i);
                        if (read!=null) {
                            producer.send(new ProducerRecord<>(topic, read.vid, read));
                        } else {
                            splitter.close();
                            splitter = new LinearRoadPreprocessing(directoryListing[0].getAbsolutePath());
                        }
                    }
                else
                    for (long i = 0; !finished; i++) {
                        SpeedEvent read = splitter.read(Duration.ofMillis(timeDiffMs).toMillis()*i);
                        if (read!=null) {
                            producer.send(new ProducerRecord<>(topic, read.vid, read));
                        } else {
                            splitter.close();
                            finished = true;
                        }
                    }



                //Closing the splitters
                for (int j=0; j < directoryListing.length; j++) {
                    splitter.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
