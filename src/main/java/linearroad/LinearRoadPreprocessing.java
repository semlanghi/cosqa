package linearroad;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Random;

public class LinearRoadPreprocessing {
    private CSVReader csvReader;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SpeedEventFactory SpeedEventFactory;
    private int inconsistencyPercentage;
    boolean injection;
    Random injectionRandom;

    public LinearRoadPreprocessing(String fileName) throws IOException, CsvValidationException {
        this.csvReader = new CSVReader(new FileReader(fileName));
        this.csvReader.readNextSilently();
        this.SpeedEventFactory = new SpeedEventFactory();
        this.injection = false;
    }

    public LinearRoadPreprocessing(String fileName, int inconsistencyPercentage) throws IOException {
        this.csvReader = new CSVReader(new FileReader(fileName));
        this.csvReader.readNextSilently();
        this.SpeedEventFactory = new SpeedEventFactory();
        this.inconsistencyPercentage = inconsistencyPercentage;
        this.injection = true;
        this.injectionRandom = new Random(System.currentTimeMillis());
    }

    public SpeedEvent read() throws Exception {
        String[] line;
        if ((line = csvReader.readNext()) != null) {
            String removedBracketsFirstField = line[0].replace("[", "").replace(" ", "");
            String removedBracketsLastField = line[line.length - 1].replace("]", "").replace(" ", "");

            if (injection) {
                int i = this.injectionRandom.nextInt(1, 101);
                if (i < inconsistencyPercentage){
                    return SpeedEventFactory.createInconsistentEvent(Long.parseLong(removedBracketsFirstField), Long.parseLong(line[8].replace(" ", "")),
                            Integer.parseInt(line[3].replace(" ", "")), Integer.parseInt(line[6].replace(" ", "")), Integer.parseInt(line[1].replace(" ", "")));
                } else {
                    return SpeedEventFactory.make(Long.parseLong(removedBracketsFirstField), Integer.parseInt(line[1].replace(" ", "")), Long.parseLong(line[8].replace(" ", "")),
                            Integer.parseInt(line[3].replace(" ", "")), Integer.parseInt(line[6].replace(" ", "")));
                }
            } else {
                return SpeedEventFactory.make(Long.parseLong(removedBracketsFirstField), Integer.parseInt(line[1].replace(" ", "")), Long.parseLong(line[8].replace(" ", "")),
                        Integer.parseInt(line[3].replace(" ", "")), Integer.parseInt(line[6].replace(" ", "")));
            }
        }
        return null;

    }

    public SpeedEvent read(long ts) throws Exception {
        String[] line;
        if ((line = csvReader.readNext()) != null) {
            String removedBracketsFirstField = line[0].replace("[", "").replace(" ", "");
            String removedBracketsLastField = line[line.length - 1].replace("]", "").replace(" ", "");

            if (injection) {
                int i = this.injectionRandom.nextInt(0, 101);
                if (i < inconsistencyPercentage){
                    return SpeedEventFactory.createInconsistentEvent(Long.parseLong(removedBracketsFirstField), ts, Integer.parseInt(line[3].replace(" ", "")), Integer.parseInt(line[6].replace(" ", "")), Integer.parseInt(line[1].replace(" ", "")));
                } else {
                    return SpeedEventFactory.make(Long.parseLong(removedBracketsFirstField), Integer.parseInt(line[1].replace(" ", "")), ts, Integer.parseInt(line[3].replace(" ", "")), Integer.parseInt(line[6].replace(" ", "")));
                }
            } else {
                return SpeedEventFactory.make(Long.parseLong(removedBracketsFirstField), Integer.parseInt(line[1].replace(" ", "")), ts, Integer.parseInt(line[3].replace(" ", "")), Integer.parseInt(line[6].replace(" ", "")));
            }
        }
        return null;
    }

    public void close() throws IOException {
        this.csvReader.close();
    }

    public static void main(String[] args){
        try {

            File dir = new File("/Users/samuelelanghi/Documents/projects/cosqa/src/main/resources/linearroad/");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {

                //Initialize vector of splitters
                LinearRoadPreprocessing[] splitters = new LinearRoadPreprocessing[directoryListing.length];
                for (int j=0; j < directoryListing.length; j++) {
                    splitters[j] = new LinearRoadPreprocessing(directoryListing[j].getAbsolutePath());
                }

                //Sending records with simulated timestamp
                boolean terminated = false;
                long countIncons = 0L;
                while (!terminated) {
                    for (int j=0; j < directoryListing.length; j++) {
                        SpeedEvent read = splitters[j].read();
                        if (read!=null){
                            if (read.speed == 0L)
                                System.out.println(read);
                        } else {
                            splitters[j].close();
                            terminated = true;
                        }
                    }
                }
                System.out.println(countIncons);

                //Closing the splitters
                for (int j=0; j < directoryListing.length; j++) {
                    splitters[j].close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
