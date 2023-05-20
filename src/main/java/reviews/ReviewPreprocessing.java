package reviews;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class ReviewPreprocessing {
    private CSVReader csvReader;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private ReviewFactory reviewFactory;

    public ReviewPreprocessing(String fileName) throws IOException, CsvValidationException {
        this.csvReader = new CSVReader(new FileReader(fileName));
        String[] split = fileName.split("/");
        this.reviewFactory = new ReviewFactory(split[split.length-1]);
    }

    public Review read() throws Exception {
        String[] line;
        if ((line = csvReader.readNext()) != null) {
            while (line.length < 4){
                line = csvReader.readNext();
                if (line == null)
                    return null;
            }
            reviewFactory.setTitle(line[1]);
            Calendar calendarStartOfTheDay = Calendar.getInstance();
            calendarStartOfTheDay.setTime(dateFormat.parse(line[2]));
            calendarStartOfTheDay.set(Calendar.HOUR_OF_DAY, 7);

            return reviewFactory.make(Long.parseLong(line[0]), Integer.parseInt(line[3]), calendarStartOfTheDay.getTimeInMillis());
        }
        return null;
    }

    public Review read(long ts) throws Exception {
        String[] line;
        if ((line = csvReader.readNext()) != null) {
            while (line.length < 4){
                line = csvReader.readNext();
                if (line == null)
                    return null;
            }
            reviewFactory.setTitle(line[1]);

            return reviewFactory.make(Long.parseLong(line[0]), Integer.parseInt(line[3]), ts);
        }
        return null;
    }

    public void close() throws IOException {
        this.csvReader.close();
    }

    public static void main(String[] args){
        try {

            File dir = new File("/Users/samuelelanghi/Documents/projects/cosqa/src/main/resources/reviews/");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {

                //Initialize vector of splitters
                ReviewPreprocessing[] splitters = new ReviewPreprocessing[directoryListing.length];
                for (int j=0; j < directoryListing.length; j++) {
                    splitters[j] = new ReviewPreprocessing(directoryListing[j].getAbsolutePath());
                }

                //Sending records with simulated timestamp
                for (long i = 0; i < 2000000; i++) {
                    for (int j=0; j < directoryListing.length; j++) {
                        Review read = splitters[j].read();
                        if (read!=null)
                            System.out.println(read);
                        else {
                            splitters[j].close();
                            splitters[j] = new ReviewPreprocessing(directoryListing[j].getAbsolutePath());
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
