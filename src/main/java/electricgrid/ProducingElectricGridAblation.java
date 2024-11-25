package electricgrid;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;
import java.util.UUID;

public class ProducingElectricGridAblation {
    public static void main(String[] args){

        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.Integer().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EGCSerde.instance().serializer().getClass());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer<Integer, EGC> producer = new KafkaProducer<>(props);

        try {
            int numEvents = Integer.parseInt(args[0]);
            String topic = "electricgrid-nrecords-"+numEvents;

            UUID conflictUUID = UUID.randomUUID();

            for (long i = 1; i <= numEvents; i++) {
                UUID uuid;
                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 50, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 30, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 20, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 20, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 40, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 60, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 80, 20, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 60, 50, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 40, 70, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 20, 100, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 20, 80, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 40, 60, i++, uuid)));

                if (i % 100 == 0) {
                    System.out.println("Conflict UUID inserted");
                    uuid = conflictUUID;
                } else {
                    uuid = UUID.randomUUID();
                }
                producer.send(new ProducerRecord<>(topic, 1, new EGC(1, 60, 40, i, uuid)));
            }


            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
