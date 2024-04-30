package electricgrid;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;
import java.util.UUID;

public class ProducingElectricGridGT {
    public static void main(String[] args){

        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.Integer().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EGCSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer<Integer, EGC> producer = new KafkaProducer<>(props);

        try {
            String topic = "gt-grid2";
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 1L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 2L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 3L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 4L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 5L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 6L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 7L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 8L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 9L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 10L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 11L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 12L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 13L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 50, 50, 14L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 30, 70, 15L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 0, 100, 16L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 20, 80, 17L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 40, 60, 18L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 60, 40, 19, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 20L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 21L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 22L, UUID.randomUUID())));
            producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, 23L, UUID.randomUUID())));


            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
