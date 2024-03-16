import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import stocks.Stock;
import stocks.StockSerde;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class ExampleTest {

    public static void main(String[] args) {
        // Set up Kafka consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockSerde.instance().deserializer().getClass());

        // Create KafkaConsumer instance
        KafkaConsumer<String, Stock> consumer = new KafkaConsumer<>(props);

        // Subscribe to the Kafka topic
        consumer.subscribe(Arrays.asList("topic"));

        // Poll for new messages
        while (true) {
            ConsumerRecords<String, Stock> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Stock> record : records) {
                System.out.println("Received message: " + record.value());
            }
        }
    }
}