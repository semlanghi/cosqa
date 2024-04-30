package electricgrid;

import java.util.UUID;

public class AvgEGC {

    int zone;
    long sumConsA;
    long sumConsB;
    long ts;

    @Override
    public String toString() {
        return "AvgEGC{" +
                "zone=" + zone +
                ", sumConsA=" + sumConsA +
                ", sumConsB=" + sumConsB +
                ", ts=" + ts +
                '}';
    }

    public AvgEGC(int zone, long sumConsA, long sumConsB, long ts) {
        this.zone = zone;
        this.sumConsA = sumConsA;
        this.sumConsB = sumConsB;
        this.ts = ts;
    }

    public int getZone() {
        return zone;
    }

    public void setZone(int zone) {
        this.zone = zone;
    }

    public long getSumConsA() {
        return sumConsA;
    }

    public void setSumConsA(long sumConsA) {
        this.sumConsA = sumConsA;
    }

    public long getSumConsB() {
        return sumConsB;
    }

    public void setSumConsB(long sumConsB) {
        this.sumConsB = sumConsB;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
