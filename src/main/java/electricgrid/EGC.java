package electricgrid;

import java.util.UUID;

public class EGC {
    int zone;
    long consA;
    long consB;
    long ts;
    UUID uuid;

    public EGC(int zone, long consA, long consB, long ts, UUID uuid) {
        this.zone = zone;
        this.consA = consA;
        this.consB = consB;
        this.ts = ts;
        this.uuid = uuid;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public int getZone() {
        return zone;
    }

    public void setZone(int zone) {
        this.zone = zone;
    }

    public long getConsA() {
        return consA;
    }

    public void setConsA(long consA) {
        this.consA = consA;
    }

    public long getConsB() {
        return consB;
    }

    public void setConsB(long consB) {
        this.consB = consB;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        return "ElectricGridConsumption{" +
                "zone=" + zone +
                ", consA=" + consA +
                ", consB=" + consB +
                ", ts=" + ts +
                ", uuid=" + uuid +
                '}';
    }
}
