package stocks;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

public class ProducingStocksSimulTime {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StockSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        long nrecords = Long.parseLong(args[0]);
        String dirPrefix = args[1];

        String topic = "stocks-nrecords-" + (nrecords == -1 ? "total" : nrecords);

        KafkaProducer<String,Stock> producer = new KafkaProducer<>(props);

        try {

            File dir = new File(dirPrefix+"/stocks/");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {

                //Initialize vector of splitters
                StockStartEndSplitter[] splitters = new StockStartEndSplitter[directoryListing.length];
                for (int j=0; j < directoryListing.length; j++) {
                    splitters[j] = new StockStartEndSplitter(directoryListing[j].getAbsolutePath());
                }

                if (nrecords > -1) {
//                    long[] startTimestamp = new long[splitters.length];
//                    long[] lastRegisteredTimestamp = new long[splitters.length];
//                    for (int i = 0; i < splitters.length; i++) {
//                        startTimestamp[i] = 0L;
//                        lastRegisteredTimestamp[i] = 0L;
//                    }
                    nrecords = nrecords / splitters.length;
                    //Sending records with simulated timestamp
                    for (long i = 0; i < nrecords; i++) {
                        for (int j = 0; j < directoryListing.length; j++) {
                            Pair<Pair<Long, Double>, Pair<Long, Double>> pair = splitters[j].read();

                            if (pair != null) {
                                Stock read = new Stock(directoryListing[j].getName().charAt(0), pair.getRight().getRight(), i * Duration.ofDays(1).toMillis());
//                                lastRegisteredTimestamp[j] = read.ts;
//                                read.addTimestamp(startTimestamp[j]);

                                producer.send(new ProducerRecord<>(topic, String.valueOf(directoryListing[j].getName().charAt(0)), read));
                            } else {
//                                startTimestamp[j] += lastRegisteredTimestamp[j];
                                splitters[j].close();
                                splitters[j] = new StockStartEndSplitter(directoryListing[j].getAbsolutePath());
                            }
                        }
                    }

                    //Closing the splitters
                    for (int j = 0; j < directoryListing.length; j++) {
                        splitters[j].close();
                    }
                    producer.close();
                } else{
                    boolean terminated = false;
                    long i = 0L;
                    while (!terminated) {
                        terminated = true;
                        for (int j = 0; j < directoryListing.length; j++) {
                            if (splitters[j]!=null){
                                terminated = false;
                                Pair<Pair<Long, Double>, Pair<Long, Double>> pair = splitters[j].read();
                                if (pair != null) {
                                    Character name = directoryListing[j].getName().charAt(0);
                                    Stock read = new Stock(name, pair.getRight().getRight(), i * Duration.ofDays(1).toMillis());
                                    producer.send(new ProducerRecord<>(topic, String.valueOf(name), read));
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
