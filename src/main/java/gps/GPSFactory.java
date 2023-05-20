package gps;

public class GPSFactory {

    public GPS make(double x, double y, long ts){
        return new GPS(x, y, ts);
    }

}
