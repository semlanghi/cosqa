package stocks;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

public class ProducingStocksExample {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StockSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer<String,Stock> producer = new KafkaProducer<>(props);
        String key = String.valueOf('A');
        try {
            String topic = "topic3";
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 12.0, 1L)));
            System.out.println("writing...");
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 10.0, 2L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 120.0, 3L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 120.0, 4L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 1200.0, 5L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 1200.0, 6L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 1200.0, 7L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 2200.0, 8L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 2200.0, 9L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 3200.0, 10L)));
            producer.send(new ProducerRecord<>(topic, key, new Stock('A', 3200.0, 11L)));

            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
