package gps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.File;
import java.util.Properties;

public class ProducingGPSDirty {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GPSSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        long nrecords = Long.parseLong(args[0]);
        String dirPrefix = args[1];

        String topic = "gps-dirty-nrecords-" + (nrecords == -1 ? "total" : nrecords);

        KafkaProducer<String, GPS> producer = new KafkaProducer<>(props);

        try {

            File dir = new File(dirPrefix+"/gps/");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {


                //Initialize vector of splitters
                GPSPreprocessing splitter = new GPSPreprocessing(directoryListing[0].getAbsolutePath());
                int counter = 0;
                if (nrecords > -1){
                    long startTimestamp = 0L;
                    long lastRegisteredTimestamp = 0L;
                    //Sending records with simulated timestamp
                    for (long i = 0; i < nrecords; i++) {
                        if (splitter != null){
                            GPS read = splitter.read();

                            if(counter < 10)
                                counter++;
                            else {
                                read.dirty();
                                counter=0;
                            }
                            if (read!=null){
                                //keep track of the last timestamp
                                lastRegisteredTimestamp = read.ts;
                                //If nrecords > fileRows, reset dataset with a new starting timestamp, i.e., the last timestamp read before ending file
                                read.addTimestamp(startTimestamp);
//                                System.out.println("Starting Production");
                                producer.send(new ProducerRecord<>(topic, read.key(), read));
                            }
                            else {
                                //increment the starting timestamp for incremental dataset reset
                                startTimestamp += lastRegisteredTimestamp;
                                splitter.close();
                                //reset splitter
                                splitter = new GPSPreprocessing(directoryListing[0].getAbsolutePath());
                            }
                        }

                    }

                    //Closing the splitter
                    splitter.close();
                } else {
                    boolean terminated = false;
                    // In this case, we simply arrive at the end of the file
                    while (!terminated) {
                        GPS read = splitter.read();

                        if (read!=null){
                            if(counter < 10)
                                counter++;
                            else {
                                read.dirty();
                                counter=0;
                            }
                            producer.send(new ProducerRecord<>(topic, read.key(), read));
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
