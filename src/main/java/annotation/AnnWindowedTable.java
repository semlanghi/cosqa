package annotation;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import topkstreaming.CADistanceBasedRanker;
import utils.ApplicationSupplier;

import java.util.Properties;

public interface AnnWindowedTable<K,V> {

    KTable<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getInternalKTable();

    KTable<Windowed<K>,Integer> top(CADistanceBasedRanker ranker, ApplicationSupplier applicationSupplier, Properties properties);

    void topKProcessorVisualizer(CADistanceBasedRanker ranker, StreamsBuilder builder, TimeWindows timeWindows, ApplicationSupplier shutdownHook, Properties properties);
}
