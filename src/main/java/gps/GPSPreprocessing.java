package gps;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class GPSPreprocessing {
    private CSVReader csvReader;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private GPSFactory GPSFactory;

    public GPSPreprocessing(String fileName) throws IOException, CsvValidationException {
        this.csvReader = new CSVReader(new FileReader(fileName));
        this.csvReader.readNext();
        this.GPSFactory = new GPSFactory();
    }

    public GPS read() throws Exception {
        String[] line;
        if ((line = csvReader.readNext()) != null) {
            return GPSFactory.make(Double.parseDouble(line[2]), Double.parseDouble(line[1]), Long.parseLong(line[0]));
        }
        return null;
    }

    public GPS read(long ts) throws Exception {
        String[] line;
        if ((line = csvReader.readNext()) != null) {
            return GPSFactory.make(Double.parseDouble(line[2]), Double.parseDouble(line[1]), ts);
        }
        return null;
    }

    public void close() throws IOException {
        this.csvReader.close();
    }

    public static void main(String[] args){
        try {

            File dir = new File("./cosqa/src/main/resources/gps/");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {

                //Initialize preprocessing objects
                GPSPreprocessing[] splitters = new GPSPreprocessing[directoryListing.length];
                for (int j=0; j < directoryListing.length; j++) {
                    splitters[j] = new GPSPreprocessing(directoryListing[j].getAbsolutePath());
                }

                //Sending records with simulated timestamp
                for (long i = 0; i < 2000000; i++) {
                    for (int j=0; j < directoryListing.length; j++) {
                        GPS read = splitters[j].read();
                        if (read!=null)
                            System.out.println(read);
                        else {
                            splitters[j].close();
                            splitters[j] = new GPSPreprocessing(directoryListing[j].getAbsolutePath());
                        }
                    }
                }

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
