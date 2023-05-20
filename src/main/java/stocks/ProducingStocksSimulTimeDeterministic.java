package stocks;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Properties;

public class ProducingStocksSimulTimeDeterministic {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StockSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        KafkaProducer<String,Stock> producer = new KafkaProducer<>(props);

        StockFactory factoryA = new StockFactory('A');
        StockFactory factoryB = new StockFactory('B');
        StockFactory factoryC = new StockFactory('C');
        StockFactory factoryD = new StockFactory('D');

        int i = 0;

        // Send consistent price fluctuation to Kafka
        String topic = "sample-topic-deterministic";
        producer.send(new ProducerRecord<>(topic, factoryA.getStockName(), factoryA.make(132.69, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryB.getStockName(), factoryB.make(223.52, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryC.getStockName(), factoryC.make(3218.51, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryD.getStockName(), factoryD.make(1751.88, Duration.ofDays(i).toMillis())));
        i++;


        // Send the price fluctuation to Kafka
        producer.send(new ProducerRecord<>(topic, factoryA.getStockName(), factoryA.make(132.69, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryB.getStockName(), factoryB.make(223.52, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryC.getStockName(), factoryC.make(3218.51, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryD.getStockName(), factoryD.make(1751.88, Duration.ofDays(i).toMillis())));
        i++;

        producer.send(new ProducerRecord<>(topic, factoryA.getStockName(), factoryA.make(131.99, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryB.getStockName(), factoryB.make(223.52, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryC.getStockName(), factoryC.make(3218.51, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryD.getStockName(), factoryD.make(1751.88, Duration.ofDays(i).toMillis())));
        i++;

        producer.send(new ProducerRecord<>(topic, factoryA.getStockName(), factoryA.make(132.05, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryB.getStockName(), factoryB.make(223.52, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryC.getStockName(), factoryC.make(3218.51, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryD.getStockName(), factoryD.make(1751.88, Duration.ofDays(i).toMillis())));
        i++;

        producer.send(new ProducerRecord<>(topic, factoryA.getStockName(), factoryA.make(129.41, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryB.getStockName(), factoryB.make(223.52, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryC.getStockName(), factoryC.make(3218.51, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryD.getStockName(), factoryD.make(1751.88, Duration.ofDays(i).toMillis())));
        i++;

        // Send inconsistent price fluctuations to Kafka
        producer.send(new ProducerRecord<>(topic, factoryA.getStockName(), factoryA.make(200.41, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryB.getStockName(), factoryB.make(400.00, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryC.getStockName(), factoryC.make(1218.51, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryD.getStockName(), factoryD.make(4751.88, Duration.ofDays(i).toMillis())));
        i++;

        // Send inconsistent price fluctuations to Kafka
        producer.send(new ProducerRecord<>(topic, factoryA.getStockName(), factoryA.make(200.41, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryB.getStockName(), factoryB.make(400.00, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryC.getStockName(), factoryC.make(1218.51, Duration.ofDays(i).toMillis())));
        producer.send(new ProducerRecord<>(topic, factoryD.getStockName(), factoryD.make(4751.88, Duration.ofDays(i).toMillis())));

        // Close the producer instance
        producer.close();

    }


}
