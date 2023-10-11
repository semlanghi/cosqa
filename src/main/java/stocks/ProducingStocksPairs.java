package stocks;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

public class ProducingStocksPairs {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.Double().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        KafkaProducer<String,Double> producer = new KafkaProducer<>(props);
        String key = "AAPL";
        for (int i = 0; i < 1000; i++) {
            try {
                StockStartEndSplitter stockStartEndSplitter = new StockStartEndSplitter("./cosqa/src/main/resources/stocks/AAPL.csv");
                Pair<Pair<Long, Double>, Pair<Long,Double>> pair = stockStartEndSplitter.read();

                while (pair != null){
                    producer.send(new ProducerRecord<>("sample-topic", key, pair.getLeft().getValue()));
                    producer.send(new ProducerRecord<>("sample-topic", key, pair.getRight().getValue()));
                    pair = stockStartEndSplitter.read();
                }
                stockStartEndSplitter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
