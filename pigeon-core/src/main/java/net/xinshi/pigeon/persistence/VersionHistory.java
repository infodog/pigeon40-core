package net.xinshi.pigeon.persistence;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-2-14
 * Time: 下午2:12
 * To change this template use File | Settings | File Templates.
 */

public class VersionHistory implements Comparable<VersionHistory> {

    private int FrontLength = 0;
    private byte[] data = null;
    private int timestamp = 0;
    private long Version = -1L;
    private int MagicNumber = -1;

    public int compareTo(VersionHistory o) {
        return (int) (Version - o.getVersion());
    }

    public VersionHistory(int frontLength, byte[] data, int timestamp, long version, int magicNumber) {
        FrontLength = frontLength;
        this.data = data;
        this.timestamp = timestamp;
        Version = version;
        MagicNumber = magicNumber;
    }

    public int getFrontLength() {
        return FrontLength;
    }

    public void setFrontLength(int frontLength) {
        FrontLength = frontLength;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public long getVersion() {
        return Version;
    }

    public void setVersion(long version) {
        Version = version;
    }

    public int getMagicNumber() {
        return MagicNumber;
    }

    public void setMagicNumber(int magicNumber) {
        MagicNumber = magicNumber;
    }

}

