package linearroad;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.File;
import java.util.Properties;

public class ProducingLinearRoadDirty {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.Long().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpeedEventSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        long nrecords = Long.parseLong(args[0]);
        String dirPrefix = args[1];
        int incons = Integer.parseInt(args[2]);

        String topic = "linearroad-nrecords-" + (nrecords == -1 ? "total" : nrecords) + "-incons-" + incons;
        KafkaProducer<Long, SpeedEvent> producer = new KafkaProducer<>(props);

        try {

            File dir = new File(dirPrefix + "/linearroad/");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {

                //Initialize preprocessor, only one file in the folder, index 0
                LinearRoadPreprocessing splitter = new LinearRoadPreprocessing(directoryListing[0].getAbsolutePath(), incons);

                //Sending records with simulated timestamp
                int counter = 10;
                if (nrecords > -1) {
                    long startTimestamp = 0L;
                    long lastRegisteredTimestamp = 0L;

                    //Sending a total of nrecords
                    for (int i = 0; i < nrecords; i++) {
                        SpeedEvent read = splitter.read();
                        if (read != null) {

                            if(counter < 10)
                                counter++;
                            else {
                                read.dirty();
                                counter=0;
                            }
                            //keep track of the last timestamp
                            lastRegisteredTimestamp = read.timestamp;
                            //If nrecords > fileRows, reset dataset with a new starting timestamp, i.e., the last timestamp read before ending file
                            read.addTimestamp(startTimestamp);

                            producer.send(new ProducerRecord<>(topic, read.vid, read));
                        } else {
                            //increment the starting timestamp for incremental dataset reset
                            startTimestamp += lastRegisteredTimestamp;
                            splitter.close();
                            //reset splitter
                            splitter = new LinearRoadPreprocessing(directoryListing[0].getAbsolutePath(), incons);
                        }
                    }
                    //Closing the splitter in case I reset
                    splitter.close();
                } else{
                    boolean terminated = false;
                    // In this case, we simply arrive at the end of the file
                    while (!terminated) {
                        SpeedEvent read = splitter.read();
                        if (read!=null){
                            if(counter < 10)
                                counter++;
                            else {
                                read.dirty();
                                counter=0;
                            }
                            producer.send(new ProducerRecord<>(topic, read.vid, read));
                        } else {
                            splitter.close();
                            terminated = true;
                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
