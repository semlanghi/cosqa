package gps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.File;
import java.util.Properties;

public class ProducingGPSDirtyManual {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GPSSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        String topic = "gps-dirty2";

        KafkaProducer<String, GPS> producer = new KafkaProducer<>(props);

        GPS gps = new GPS(10.0, 10.0, 1);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(15.0, 15.0, 2);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(15.0, 20.0, 3);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(20.0, 20.0, 4);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(25.0, 25.0, 5);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(30.0, 25.0, 6);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(40.0, 40.0, 7);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(45.0, 45.0, 8);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(45.0, 50.0, 9);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(60.0, 60.0, 10);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        gps = new GPS(75.0, 75.0, 11);
        producer.send(new ProducerRecord<>(topic, gps.key(), gps));
        producer.flush();

    }
}
