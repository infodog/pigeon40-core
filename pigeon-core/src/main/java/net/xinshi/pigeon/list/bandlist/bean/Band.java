package net.xinshi.pigeon.list.bandlist.bean;

import net.xinshi.pigeon.list.SortListObject;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-9
 * Time: 下午2:36
 * To change this template use File | Settings | File Templates.
 */

public class Band {
    long id;
    String listName;
    int isHead;
    String value;
    int isDirty;
    int isMeta;
    long nextMetaBandId;
    long prevMetaBandId;
    String minKey;
    String maxKey;
    List<BandInfo> bandInfos;
    List objList;
    long version;
    int hash;
    long txid;

    public int getHash() {
        return hash;
    }

    public void setHash(int hash) {
        this.hash = hash;
    }

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    final static String BAND_SEPERATOR = "\n";

    public boolean isEmpty() {
        if (getMeta() == 0) {
            if (this.getObjList().size() == 0) {
                return true;
            } else {
                return false;
            }
        } else {
            if (this.getBandInfos().size() == 0) {
                return true;
            } else {
                return false;
            }
        }
    }

    public Band clone() {
        Band newBand = null;
        try {
            newBand = (Band) BeanUtils.cloneBean(this);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        if (bandInfos != null) {
            Vector<BandInfo> newBandInfos = new Vector<BandInfo>();
            for (BandInfo info : bandInfos) {
                try {
                    newBandInfos.add(info.clone());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            newBand.setBandInfos(newBandInfos);
        }
        if (objList != null) {
            Vector<SortListObject> newObjList = new Vector<SortListObject>();
            for (Object obj : objList) {
                try {
                    SortListObject sobj = ((SortListObject) obj).clone();
                    newObjList.add(sobj);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            newBand.setObjList(newObjList);
        }
        return newBand;
    }

    public String getMinKey() {
        return minKey;
    }

    public void setMinKey(String minKey) {
        this.minKey = minKey;
    }

    public String getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(String maxKey) {
        this.maxKey = maxKey;
    }

    public static class BandInfo {
        long bandId;
        long number;
        String minKey;
        String maxKey;

        public String getMinKey() {
            return minKey;
        }

        public void setMinKey(String minKey) {
            this.minKey = minKey;
        }

        public String getMaxKey() {
            return maxKey;
        }

        public void setMaxKey(String maxKey) {
            this.maxKey = maxKey;
        }

        public long getBandId() {
            return bandId;
        }

        public void setBandId(long bandId) {
            this.bandId = bandId;
        }

        public long getNumber() {
            return number;
        }

        public void setNumber(long number) {
            this.number = number;
        }

        public BandInfo clone() {
            BandInfo newInfo = new BandInfo();
            newInfo.bandId = this.bandId;
            newInfo.number = this.number;
            newInfo.minKey = this.minKey;
            newInfo.maxKey = this.maxKey;
            return newInfo;
        }
    }

    public List<BandInfo> getBandInfos() {
        if (bandInfos == null && this.getMeta() == 1) {
            bandInfos = new Vector();
        }
        return bandInfos;
    }

    public void setBandInfos(List<BandInfo> bandInfos) {
        this.bandInfos = bandInfos;
    }

    public int getDirty() {
        return isDirty;
    }

    public void setDirty(int dirty) {
        isDirty = dirty;
    }

    public List getObjList() {
        if (objList == null) {
            objList = new Vector();
        }
        return objList;
    }

    public void setObjList(List objList) {
        this.objList = objList;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getListName() {
        return listName;
    }

    public void setListName(String listName) {
        this.listName = listName;
    }

    public int getHead() {
        return isHead;
    }

    public void setHead(int head) {
        isHead = head;
    }

    public String getDirtyValue() {
        String value;
        StringBuilder strBuilder = new StringBuilder();
        if (getMeta() == 0) {
            for (SortListObject sortObj : (List<SortListObject>) getObjList()) {
                strBuilder.append(sortObj.getObjid()).append(",").append(sortObj.getKey()).append(";");
            }
            strBuilder.append(BAND_SEPERATOR);
            value = strBuilder.toString();
        } else {
            for (Band.BandInfo info : getBandInfos()) {
                strBuilder.append(info.getBandId()).append(",").append(info.getNumber()).append(",");
                strBuilder.append(info.getMinKey()).append(",").append(info.getMaxKey()).append(";");
            }
            strBuilder.append(BAND_SEPERATOR);
            value = strBuilder.toString();
        }
        return value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getMeta() {
        return isMeta;
    }

    public void setMeta(int meta) {
        isMeta = meta;
    }

    public long getNextMetaBandId() {
        return nextMetaBandId;
    }

    public void setNextMetaBandId(long nextMetaBandId) {
        this.nextMetaBandId = nextMetaBandId;
    }

    public long getPrevMetaBandId() {
        return prevMetaBandId;
    }

    public void setPrevMetaBandId(long prevMetaBandId) {
        this.prevMetaBandId = prevMetaBandId;
    }

}

