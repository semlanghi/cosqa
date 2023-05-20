package linearroad;


public class SpeedEventFactory {

    long sum = 0;
    long count = 0;
    boolean positive = true;
    int multiplier = 1;

    public SpeedEventFactory() {
    }

    public SpeedEvent make(long vid, int speed, long ts, int xWay, int segment){
        sum+=speed;
        count++;
        return new SpeedEvent(ts, speed, vid, xWay, segment);
    }

    public SpeedEvent createInconsistentEvent(long vid, long ts, int xWay, int segment, int speed){
        sum+=speed;
        count++;
        if (multiplier > 50)
            multiplier = 1;
        if (positive){
            positive = false;
            return new SpeedEvent(ts, (int) ((sum / count) * multiplier++), vid, xWay, segment);
        } else {
            positive = true;
            return new SpeedEvent(ts, (int) -((sum / count) * multiplier++), vid, xWay, segment);
        }
    }

}
