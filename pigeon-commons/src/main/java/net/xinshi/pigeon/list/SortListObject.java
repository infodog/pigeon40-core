package net.xinshi.pigeon.list;


/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-11-22
 * Time: 14:58:50
 * To change this template use File | Settings | File Templates.
 */

public class SortListObject implements Comparable<SortListObject> {
    String key;
    String objid;
    String wholeKey;
    long txid;

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
    }

    public final static String KEY_DELIMITER = ":";

    public SortListObject() {

    }

    public SortListObject(String wholeObject) {
        if (wholeObject == null) {
            key = "";
            objid = "";

        } else {
            String[] parts = wholeObject.split(SortListObject.KEY_DELIMITER);
            if (parts.length > 1) {
                key = parts[0];
                objid = parts[1];
            } else {
                key = parts[0];
                objid = "";

            }
        }
    }

    public SortListObject clone() {
        SortListObject newObj = new SortListObject();
        newObj.key = key;
        newObj.objid = objid;
        newObj.wholeKey = wholeKey;
        return newObj;
    }

    public SortListObject(String key, String objId) {
        this.key = key;
        this.objid = objId;
    }

    public SortListObject(String key, String objId, long txid){
        this.key = key;
        this.objid = objId;
        this.txid = txid;
    }

    public String getWholeKey() {
        if (wholeKey == null) {
            wholeKey = key + KEY_DELIMITER + objid;
        }
        return wholeKey;
    }


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        wholeKey = null;
        this.key = key;
    }

    public String getObjid() {
        return objid;
    }

    public void setObjid(String objid) {
        wholeKey = null;
        this.objid = objid;
    }


    public int compareTo(SortListObject o) {

        assert (this.key != null);
        assert (this.objid != null);
        assert (o.key != null);
        assert (o.objid != null);
        int r = key.compareTo(o.getKey());
        if (r != 0) {
            return r;
        }
        return objid.compareTo(o.getObjid());
    }
}
