package stocks;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class DumpingStocks {
    static Logger logger = Logger.getLogger(DumpingStocks.class.getName());
    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockSerde.instance().deserializer().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        KafkaConsumer<String, Stock> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("sample-topic"));
        int i=0;
        while (i<200000){
            ConsumerRecords<String,Stock> records = consumer.poll(Duration.ZERO);
            records.forEach(new Consumer<ConsumerRecord<String, Stock>>() {
                @Override
                public void accept(ConsumerRecord<String, Stock> stringStockConsumerRecord) {
                    logger.info(String.valueOf(stringStockConsumerRecord));
                }
            });
            i++;
        }
    }
}
