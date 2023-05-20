package topkstreaming;

import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import com.opencsv.CSVWriter;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Properties;

public class TopKCAProcessorNotWindowedNI<K,V> extends ContextualProcessor<K, V, Void, Void> {


    private KeyValueStore<Windowed<K>, V> topKStore;
    private final String topKStoreName;
    private AnnotationAwareTimeWindows timeWindows;
    private final String resultFileSuffix;
    private final String performanceFileDirectory;
    private long count = 0;
    private final long granularity;
    private long timeStart;
    private ApplicationSupplier shutdownHook;
    private final long maxEvents;
    private boolean finished = false;
    private File file;
    private final int percentageOfInconsistency;
    private final int constraintStrictness;
    private final long windowSizeMs;
    private final long windowSlideMs;
    private long memorySizeSum = 0;
    private long memorySizeMax = 0;
    private double cpuUtilizationSum = 0.0;
    private double cpuUtilizationMax = 0.0;
    private long numberOfInconsistencies;

    public TopKCAProcessorNotWindowedNI(String topKStoreName, AnnotationAwareTimeWindows timeWindows, ApplicationSupplier shutdownHook, Properties properties) {
        this.topKStoreName = topKStoreName;
        this.timeWindows = timeWindows;
        this.resultFileSuffix = properties.getProperty(ExperimentConfig.RESULT_FILE_SUFFIX);
        this.shutdownHook = shutdownHook;
        this.maxEvents = Long.parseLong(properties.getProperty(ExperimentConfig.EVENTS_MAX));
        this.windowSizeMs = Long.parseLong(properties.getProperty(ExperimentConfig.WINDOW_SIZE_MS));
        this.windowSlideMs = Long.parseLong(properties.getProperty(ExperimentConfig.WINDOW_SLIDE_MS));
        this.percentageOfInconsistency = Integer.parseInt(properties.getProperty(ExperimentConfig.INCONSISTENCY_PERCENTAGE));
        this.constraintStrictness = Integer.parseInt(properties.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        this.granularity = Long.parseLong(properties.getProperty(ExperimentConfig.EVENTS_GRANULARITY, "-1L"));
        this.performanceFileDirectory = properties.getProperty(ExperimentConfig.RESULT_FILE_DIR);
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        super.init(context);
        topKStore = context.getStateStore(topKStoreName);
        this.timeStart = System.currentTimeMillis();
        file = new File(performanceFileDirectory+ "throughput-"+ resultFileSuffix +".csv");
        boolean fileExists = file.exists();
        String[] fields = {"ExperimentID", "PercentageOfInc", "ConstraintStrictness", "WindowSizeMs",
                "WindowSlideMs", "StartTimeMs", "CurrentTimeMs", "NumberOfEvents", "AvgMemory", "MaxMemory",
                "AvgCPU", "MaxCPU", "NumberOfInconsistencies"};
        if (!fileExists){
            try {
                boolean fileOk = file.createNewFile();
                if (fileOk){
                    FileWriter fileWriter = new FileWriter(file, true);
                    CSVWriter csvWriter = new CSVWriter(fileWriter);
                    csvWriter.writeNext(fields);
                    csvWriter.flush();
                    csvWriter.close();
                    fileWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void process(Record<K, V> record) {

        Windowed<K> key = new Windowed<>(record.key(), timeWindows.windowsFor(record.timestamp()).values().iterator().next());
        topKStore.put(key, record.value());
        KeyValueIterator<Windowed<K>, V> range = topKStore.range(key, key);

//        Not used for evaluation
//        int rank = 1;
//        while (range.hasNext()){
//            //forwardElement
//            KeyValue<Windowed<K>, V> next = range.next();
////            context().forward(new Record<>(next.key, rank, context().currentStreamTimeMs()));
//            System.out.println(new Record<>(next.key, rank, context().currentStreamTimeMs()));
//            rank++;
//        }

        registerPerf(record);
    }

    private void registerPerf(Record<K,V> record) {
        try {
            long memorySize = getMemorySize();
            double cpuUtilization = getCpuUtilization();
            if (memorySize>memorySizeMax)
                memorySizeMax = memorySize;
            if (cpuUtilization>cpuUtilizationMax)
                cpuUtilizationMax = cpuUtilization;
            memorySizeSum+=memorySize;
            cpuUtilizationSum+=cpuUtilization;
            numberOfInconsistencies += 0;

            count++;
            if (count%granularity==0 && granularity!=-1){
                register();
            }
            if(count > maxEvents && !finished){
                finished = true;
                register();
                shutdownHook.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void register() throws IOException {
        long currentTime = System.currentTimeMillis();
        FileWriter fileWriter = new FileWriter(file, true);
        CSVWriter csvWriter = new CSVWriter(fileWriter);
        String[] fields = {this.context().applicationId(), String.valueOf(percentageOfInconsistency),
                String.valueOf(constraintStrictness), String.valueOf(windowSizeMs),
                String.valueOf(windowSlideMs), String.valueOf(timeStart), String.valueOf(currentTime),
                String.valueOf(count), String.valueOf(memorySizeSum/count), String.valueOf(memorySizeMax),
                String.valueOf(cpuUtilizationSum/count), String.valueOf(cpuUtilizationMax),
                String.valueOf(numberOfInconsistencies)};
        csvWriter.writeNext(fields);
        csvWriter.flush();
        csvWriter.close();
        fileWriter.close();
    }

    private long getMemorySize() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory();
    }

    private double getCpuUtilization() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        if (threadMXBean.isThreadCpuTimeSupported() && threadMXBean.isThreadCpuTimeEnabled()) {
            long[] threadIds = threadMXBean.getAllThreadIds();
            long totalCpuTime = 0L;
            long totalUserTime = 0L;
            for (long threadId : threadIds) {
                totalCpuTime += threadMXBean.getThreadCpuTime(threadId);
                totalUserTime += threadMXBean.getThreadUserTime(threadId);
            }
            return (double) totalUserTime / totalCpuTime;
        } else {
            return 0.0;
        }
    }

    @Override
    public void close() {
        topKStore.close();
    }


}
