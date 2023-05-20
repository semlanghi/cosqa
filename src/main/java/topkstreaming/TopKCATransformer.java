package topkstreaming;

import annotation.ConsistencyAnnotatedRecord;
import com.opencsv.CSVWriter;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Properties;

public class TopKCATransformer<K,V> implements ValueTransformerWithKey<Windowed<K>, ConsistencyAnnotatedRecord<V>, Integer> {

    private ProcessorContext processorContext;
    private KeyValueStore<Windowed<K>, ConsistencyAnnotatedRecord<V>> topKStore;
    private final String topKStoreName;
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

    public TopKCATransformer(String topKStoreName, ApplicationSupplier shutdownHook, Properties properties) {
        this.topKStoreName = topKStoreName;
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
    public void init(ProcessorContext context) {
        processorContext = context;
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
    public Integer transform(Windowed<K> readOnlyKey, ConsistencyAnnotatedRecord<V> value) {
        topKStore.put(readOnlyKey, value);
        KeyValueIterator<Windowed<K>, ConsistencyAnnotatedRecord<V>> range = topKStore.range(readOnlyKey, readOnlyKey);

//        Ranking, but for the evaluation not needed
//        int rank = 0;
//        while (range.hasNext()){
//            //Log the element
//            if (!range.next().value.equals(value))
//                rank++;
//            else return rank;
//        }
//        return null;

        registerPerf(value);
        return 1;

    }

    private void registerPerf(ConsistencyAnnotatedRecord<V> value){
        try {
            long memorySize = getMemorySize();
            double cpuUtilization = getCpuUtilization();
            if (memorySize>memorySizeMax)
                memorySizeMax = memorySize;
            if (cpuUtilization>cpuUtilizationMax)
                cpuUtilizationMax = cpuUtilization;
            memorySizeSum+=memorySize;
            cpuUtilizationSum+=cpuUtilization;
            numberOfInconsistencies += value.getPolynomial()
                    .getMonomials().stream()
                    .reduce(0L,
                            (aLong, monomial) -> monomial.getCardinality() + aLong,
                            Long::sum);

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
        String[] fields = {processorContext.applicationId(), String.valueOf(percentageOfInconsistency),
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
