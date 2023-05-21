package annotation;

import annotation.polynomial.Monomial;
import annotation.polynomial.MonomialImplString;
import annotation.polynomial.Polynomial;
import reviews.Review;
import reviews.ReviewSerde;
import stocks.Stock;
import stocks.StockSerde;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsistencyAnnotatedRecord<V> {
    private Polynomial polynomial;
    private final V wrappedRecord;

    public ConsistencyAnnotatedRecord(V wrappedRecord) {
        this.wrappedRecord = wrappedRecord;
        this.polynomial = new Polynomial();
    }

    public ConsistencyAnnotatedRecord(Polynomial polynomial, V wrappedRecord) {
        this.wrappedRecord = wrappedRecord;
        this.polynomial = polynomial;
    }

    public <VR> ConsistencyAnnotatedRecord<VR> withRecord(VR nwRecord){
        return new ConsistencyAnnotatedRecord<>(polynomial, nwRecord);
    }

    public ConsistencyAnnotatedRecord<V> withPolynomial(Polynomial nwPoly){
        return new ConsistencyAnnotatedRecord<>(nwPoly, wrappedRecord);
    }

    public static <V> ConsistencyAnnotatedRecord<V> makeCopyOf(ConsistencyAnnotatedRecord<V> consistencyAnnotatedRecord){
        return new ConsistencyAnnotatedRecord<>(consistencyAnnotatedRecord.getWrappedRecord());
    }

    public Polynomial getPolynomial() {
        return polynomial;
    }

    public void setPolynomial(Polynomial polynomial) {
        this.polynomial = polynomial;
    }

    public V getWrappedRecord() {
        return wrappedRecord;
    }

    public static AnnotatedRecordSerdePairDoubleLong serdePairDoubleLong(){
        return new AnnotatedRecordSerdePairDoubleLong();
    }

    public static <V> Serde<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> serde(Serde<V> internalSerde){
        return new ConsistencyAnnotatedRecordSerde<>(internalSerde);
    }

    @Override
    public String toString() {
        return "ConsistencyAnnotatedRecord{" +
                "polynomial=" + polynomial +
                ", wrappedRecord=" + wrappedRecord +
                '}';
    }

    private static class AnnotatedRecordSerdePairDoubleLong implements Serde<ConsistencyAnnotatedRecord<Pair<Double, Long>>>{


        @Override
        public Serializer<ConsistencyAnnotatedRecord<Pair<Double, Long>>> serializer() {
            return new Serializer<ConsistencyAnnotatedRecord<Pair<Double, Long>>>() {

                @Override
                public byte[] serialize(String topic, ConsistencyAnnotatedRecord<Pair<Double, Long>> data) {
                    try {
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                        objectOutputStream.writeDouble(data.wrappedRecord.getKey());
                        objectOutputStream.writeLong(data.wrappedRecord.getValue());
                        
                        objectOutputStream.writeObject(data.getPolynomial());

                        return byteArrayOutputStream.toByteArray();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
        }

        @Override
        public Deserializer<ConsistencyAnnotatedRecord<Pair<Double, Long>>> deserializer() {
            return new Deserializer<ConsistencyAnnotatedRecord<Pair<Double, Long>>>() {
                @Override
                public ConsistencyAnnotatedRecord<Pair<Double, Long>> deserialize(String topic, byte[] data) {
                    try {
                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
                        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);

                        double value = objectInputStream.readDouble();
                        long timestamp = objectInputStream.readLong();
                        Pair<Double, Long> wrappedRecord = new ImmutablePair<>(value, timestamp);
                        Polynomial polynomial = (Polynomial) objectInputStream.readObject();

                        return new ConsistencyAnnotatedRecord<>(
                                polynomial, wrappedRecord);

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
        }
    }


    private static class ConsistencyAnnotatedRecordSerde<V> implements Serde<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>{
        private final Serde<ValueAndTimestamp<V>> internalSerde;
        public ConsistencyAnnotatedRecordSerde(Serde<V> internalSerde) {
            this.internalSerde = new ValueAndTimestampSerde<>(internalSerde);
        }


        @Override
        public Serializer<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> serializer() {
            return new Serializer<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>() {


                @Override
                public byte[] serialize(String topic, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> data) {
                    try {
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
//                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//                        objectOutputStream.writeInt(data.getPolynomial().getDegree());
//                        objectOutputStream.writeInt(data.getPolynomial().getMonomialsDegreeSum());
//                        objectOutputStream.writeObject(data.getPolynomial().getMonomials());

                        String s = data.getPolynomial().toString() + "\n";
                        int size = s.getBytes().length;
                        dataOutputStream.writeInt(size);
                        byteArrayOutputStream.write(s.getBytes());
                        byteArrayOutputStream.write(internalSerde.serializer().serialize(topic, data.getWrappedRecord()));

//                        objectOutputStream.close();
                        dataOutputStream.close();
                        byteArrayOutputStream.close();

                        return byteArrayOutputStream.toByteArray();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
        }

        @Override
        public Deserializer<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> deserializer() {
            return new Deserializer<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>() {
                public Polynomial parsePolynomial(String polynomialStr) {
                    String termPattern = "(<.*?>)(\\^)(\\d+)";
                    Pattern pattern = Pattern.compile(termPattern);

                    String[] terms = polynomialStr.split("\\+");
                    Polynomial polynomial = null;

                    for (String term : terms) {
                        String coefficientStr = term.split("<")[0];
                        int coefficient = (coefficientStr != null && !coefficientStr.isEmpty()) ? Integer.parseInt(coefficientStr) : 1;

                        Matcher matcher = pattern.matcher(term);
                        Monomial<String> nwMonomial = new MonomialImplString(coefficient);

                        matcher.results().forEachOrdered(new Consumer<MatchResult>() {
                            @Override
                            public void accept(MatchResult matchResult) {
                                String variableStr = matchResult.group(1);
                                String exponentStr = matchResult.group(3);


                                int exponent = (exponentStr != null && !exponentStr.isEmpty()) ? Integer.parseInt(exponentStr) : 1;

                                nwMonomial.times(variableStr, exponent);
                            }
                        });



                        if (polynomial==null)
                            polynomial = new Polynomial(nwMonomial);
                        else polynomial = polynomial.plus(new Polynomial(nwMonomial));
                    }

                    return polynomial;
                }

                @Override
                public ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> deserialize(String topic, byte[] data) {
                    try {

                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
                        InputStreamReader in = new InputStreamReader(byteArrayInputStream);
                        BufferedReader d = new BufferedReader(in);
                        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

//                        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
//                        int degree = objectInputStream.readInt();
//                        int monomialsDegreeSum = objectInputStream.readInt();
//                        List<Monomial> monomials = (List<Monomial>) objectInputStream.readObject();
//                        Polynomial polynomial = new Polynomial(monomials, degree, monomialsDegreeSum);

//                        long timestamp = objectInputStream.readLong();
                        //(Polynomial) objectInputStream.readObject();
                        int size = dataInputStream.readInt() + 4;
                        Polynomial polynomial = parsePolynomial(d.readLine());
                        byteArrayInputStream.reset();
                        byteArrayInputStream.skip(size);
                        ValueAndTimestamp<V> valueAndTimestamp = internalSerde.deserializer().deserialize(topic, byteArrayInputStream.readAllBytes());


                        d.close();
//                        objectInputStream.close();
                        dataInputStream.close();
                        byteArrayInputStream.close();

                        return new ConsistencyAnnotatedRecord<>(
                                polynomial, valueAndTimestamp);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
        }
    }

    public static void main(String[] args){

        Monomial monomial = new MonomialImplString("<xcsdcsdc cdcsd>", 2);
        Polynomial polynomial = new Polynomial(monomial);
        System.out.println(polynomial);

        Monomial monomial1 = new MonomialImplString("<csacd y>", 3333);
        Monomial monomial2 = new MonomialImplString("<csacdx>", 2);
//        System.out.println(polynomial.plus(new Polynomial(monomial1)).times(monomial2));

        Stock stock = new Stock('A', 12.3, 12L);
        Review review = new Review(120030L, "finalfantasy", 12345L, 5);
        Review review1 = new Review(120030L, "finalfantasy", 12345L, 5);
        Review review2 = new Review(120030L, "finalfantasy", 12345L, 5);
        ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> annM1Stock = new ConsistencyAnnotatedRecord<>(new Polynomial(monomial1).plus(new Polynomial(monomial1)), ValueAndTimestamp.make(stock, 12L));
        ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>> annM1Review = new ConsistencyAnnotatedRecord<>(new Polynomial(monomial1).plus(new Polynomial(monomial2)), ValueAndTimestamp.make(review, 12L));
//        Stock stock1 = new Stock('G', 27.3, 13L);
//        Stock stock2 = new Stock('G', 56.4, 27L);

        Serde<Stock> serde = StockSerde.instance();
        Serde<Review> serdeRev = ReviewSerde.instance();
        Serde<ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>> serdeAnnStock = ConsistencyAnnotatedRecord.serde(serde);
        Serde<ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>> serdeAnnReview = ConsistencyAnnotatedRecord.serde(serdeRev);

        byte[] stockByte = serdeAnnStock.serializer().serialize("csbdjc", annM1Stock);
        byte[] reviewByte = serdeAnnReview.serializer().serialize("csbdjc", annM1Review);
//        byte[] stock1Byte = serde.serializer().serialize("csbdjc", stock1);
//        byte[] stock2Byte = serde.serializer().serialize("csbdjc", stock2);

        System.out.println(serdeAnnStock.deserializer().deserialize("csbdjc", stockByte));
        System.out.println(serdeAnnReview.deserializer().deserialize("csbdjc", reviewByte));
//        System.out.println(serde.deserializer().deserialize("csbdjc", stock1Byte));
//        System.out.println(serde.deserializer().deserialize("csbdjc", stock2Byte));

    }
}
