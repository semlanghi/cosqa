package stocks;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class StockStartEndSplitter {
    private CSVReader csvReader;
    private final DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");

    public StockStartEndSplitter(String fileName) throws IOException, CsvValidationException {
        this.csvReader = new CSVReader(new FileReader(fileName));
        this.csvReader.readNext();
    }

    public Pair<Pair<Long, Double>, Pair<Long, Double>> read() throws Exception {
        String[] line;
        if ((line = csvReader.readNext()) != null) {
            Calendar calendarStartOfTheDay = Calendar.getInstance();
            calendarStartOfTheDay.setTime(dateFormat.parse(line[0]));
            calendarStartOfTheDay.set(Calendar.HOUR_OF_DAY, 7);
            Pair<Long, Double> firstPair = new ImmutablePair<>(calendarStartOfTheDay.getTimeInMillis(), Double.parseDouble(line[2]));

            Calendar calendarEndOfTheDay = Calendar.getInstance();
            calendarEndOfTheDay.setTime(dateFormat.parse(line[0]));
            calendarEndOfTheDay.set(Calendar.HOUR_OF_DAY, 20);
            Pair<Long, Double> secondPair = new ImmutablePair<>(calendarEndOfTheDay.getTimeInMillis(), Double.parseDouble(line[5]));

            return new ImmutablePair<>(firstPair, secondPair);
        }
        return null;
    }

    public void close() throws IOException {
        this.csvReader.close();
    }

    public static void main(String[] args){
        try {
            StockStartEndSplitter stockStartEndSplitter = new StockStartEndSplitter("/Users/samuelelanghi/Documents/projects/cosqa/src/main/resources/stocks/AAPL.csv");
            Pair<Pair<Long, Double>, Pair<Long, Double>> pair = stockStartEndSplitter.read();
            while (pair!= null){
                System.out.println(pair);
                pair = stockStartEndSplitter.read();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
