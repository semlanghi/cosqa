package utils;

import linearroad.SpeedEvent;
import linearroad.SpeedEventSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import reviews.Review;
import reviews.ReviewSerde;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class CountingConsumerLinearRoad {
    private static final String TOPIC = "linearroad-nrecords-2000000-incons-0";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = UUID.randomUUID().toString();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpeedEventSerde.instance().deserializer().getClass().getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, SpeedEvent> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        int recordCount = 0;

        try {
            while (true) {
                ConsumerRecords<String, SpeedEvent> records = consumer.poll(100);

                for (ConsumerRecord<String, SpeedEvent> record : records) {
                    // Process the record as needed
                    System.out.println("Received "+recordCount+" record: " + record.value());
                    recordCount++;
                }

                consumer.commitSync();
            }
        } finally {
            consumer.close();
            System.out.println("Total records consumed: " + recordCount);
        }
    }
}
