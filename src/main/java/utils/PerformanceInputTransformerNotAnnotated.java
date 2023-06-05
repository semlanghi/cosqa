package utils;

import com.opencsv.CSVWriter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Properties;

public class PerformanceInputTransformerNotAnnotated<Kin, Vin> implements Transformer<Kin, Vin, KeyValue<Kin, Vin>> {
    private final String resultFileSuffix;
    private final String performanceFileDirectory;
    private long count = 0;
    private final long granularity;
    private org.apache.kafka.streams.processor.ProcessorContext context;
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



    public PerformanceInputTransformerNotAnnotated(ApplicationSupplier shutdownHook, Properties properties) {
        this.resultFileSuffix = properties.getProperty(ExperimentConfig.RESULT_FILE_SUFFIX);
        this.shutdownHook = shutdownHook;
        this.maxEvents = Long.parseLong(properties.getProperty(ExperimentConfig.EVENTS_MAX));
        this.windowSizeMs = Long.parseLong(properties.getProperty(ExperimentConfig.WINDOW_SIZE_MS));
        this.windowSlideMs = Long.parseLong(properties.getProperty(ExperimentConfig.WINDOW_SLIDE_MS));
        this.percentageOfInconsistency = Integer.parseInt(properties.getProperty(ExperimentConfig.INCONSISTENCY_PERCENTAGE));
        this.constraintStrictness = Integer.parseInt(properties.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        this.granularity = -1;
        this.performanceFileDirectory = properties.getProperty(ExperimentConfig.RESULT_FILE_DIR);
    }


    private void register() throws IOException {
        long currentTime = System.currentTimeMillis();
        FileWriter fileWriter = new FileWriter(file, true);
        CSVWriter csvWriter = new CSVWriter(fileWriter);
        String[] fields = {context.applicationId(), String.valueOf(percentageOfInconsistency),
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
    public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
        this.context = context;
        this.timeStart = System.currentTimeMillis();
        file = new File(performanceFileDirectory+ "throughput-input-"+ resultFileSuffix +".csv");
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
    public KeyValue<Kin,Vin> transform(Kin key, Vin value) {
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
            return new KeyValue<>(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
