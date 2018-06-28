package net.xinshi.pigeon.list.bandlist;

import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.list.bandlist.bean.Band;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-9
 * Time: 下午3:00
 * To change this template use File | Settings | File Templates.
 */

public class SortBandList implements ISortList {
    IListBandService bandService;
    List<Band> metaBands;
    String listId;
    long size;
    Band headBand;
    Logger logger = Logger.getLogger(SortBandList.class.getName());

    public final static String OP_ADD = "ADD";
    public final static String OP_DELETE = "DEL";
    public final static String OP_REORDER = "REO";

    public void setHeadBand(Band headBand) {
        this.headBand = headBand;
    }

    public Band get_HeadBand() {
        return headBand;
    }

    public IListBandService getBandService() {
        return bandService;
    }

    public void setBandService(IListBandService bandService) {
        this.bandService = bandService;
    }

    public List<Band> getMetaBands() {
        return metaBands;
    }

    public void setMetaBands(List<Band> metaBands) {
        this.metaBands = metaBands;
    }

    public String getListId() {
        return listId;
    }

    public void setListId(String listId) {
        this.listId = listId;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    private String getMetaBandString(Band metaBand) {
        StringBuffer sb = new StringBuffer();
        sb.append("id=" + metaBand.getId()).append(";");
        sb.append("nextId=" + metaBand.getNextMetaBandId()).append(";");
        sb.append("prevId=" + metaBand.getPrevMetaBandId()).append(";");
        return sb.toString();
    }

    private String getMetaBandsString() {
        StringBuffer sb = new StringBuffer();
        for (Band metaBand : this.metaBands) {
            sb.append(getMetaBandString(metaBand)).append("\n");
        }
        return sb.toString();
    }

    synchronized public List<SortListObject> getRange(int fromIndex, int number) throws Exception {
        if (number == 0) {
            return new Vector();
        }
        if (number == -1) {
            number = (int) size;
        }
        if (fromIndex >= size) {
            return new Vector();
        }
        int toIndex = fromIndex + number;
        if (toIndex > size) {
            toIndex = (int) size;
        }
        Vector result = new Vector();
        Band hband = this.getHeadBand();
        if (hband.getMeta() == 0) {
            result.addAll(hband.getObjList().subList(fromIndex, toIndex));
            return result;
        } else {
            int curIndex = 0;
            boolean found = false;
            for (Band metaBand : this.metaBands) {
                for (Band.BandInfo bandInfo : metaBand.getBandInfos()) {
                    if (bandInfo.getNumber() + curIndex > fromIndex) {
                        found = true;
                    }
                    if (found) {
                        Band databand = bandService.getBandById(this, bandInfo.getBandId());
                        int beginIdx;
                        int endIdx;
                        if (curIndex > fromIndex) {
                            beginIdx = 0;
                        } else {
                            beginIdx = fromIndex - curIndex;
                        }
                        if (curIndex + bandInfo.getNumber() > toIndex) {
                            endIdx = toIndex - curIndex;
                        } else {
                            endIdx = (int) bandInfo.getNumber();
                        }
                        if (endIdx > databand.getObjList().size()) {
                            throw new Exception("Internal Error,endIdx=" + endIdx + ", databand size=" + databand.getObjList().size() + ",bandInfo.number = " + bandInfo.getNumber() + ",databand id=" + databand.getId() + ",metaband.id =" + metaBand.getId() + "\n" + getMetaBandsString());
                        }
                        result.addAll(databand.getObjList().subList(beginIdx, endIdx));
                    }
                    curIndex += bandInfo.getNumber();
                    if (curIndex >= toIndex) {
                        return result;
                    }
                }
            }
        }
        throw new Exception("internal data structure corrupted. ListId=" + this.getListId());
    }

    synchronized boolean internal_delete(SortListObject sortObj) throws Exception {
        Pos pos = search(sortObj);
        if (pos.found == false) {
            return false;
        }
        size--;
        Band hband = getHeadBand();
        if (hband.getMeta() == 0) {
            hband.getObjList().remove(pos.dataIndex);
            hband.setTxid(sortObj.getTxid());
            bandService.putToDirtyBandList(hband);
            return true;
        }
        Band metaBand = metaBands.get(pos.metaBandIndex);
        Band.BandInfo bandInfo = metaBand.getBandInfos().get(pos.bandInfoIndex);
        Band dataBand = bandService.getBandById(this, bandInfo.getBandId());
        dataBand.getObjList().remove(pos.dataIndex);
        if (dataBand.getObjList().size() == 0) {
            metaBand.getBandInfos().remove(pos.bandInfoIndex);
            updateMetaBandRange(metaBand);
            if (metaBand.getBandInfos().size() == 0) {
                metaBand.setTxid(sortObj.getTxid());
                removeMetaBand(metaBand, pos.metaBandIndex);
            }
            dataBand.setTxid(sortObj.getTxid());
            metaBand.setTxid(sortObj.getTxid());
            bandService.putToDirtyBandList(dataBand);

            bandService.putToDirtyBandList(metaBand);
        } else {
            this.updateBandInfo(dataBand, bandInfo);
            updateMetaBandRange(metaBand);
            dataBand.setTxid(sortObj.getTxid());
            metaBand.setTxid(sortObj.getTxid());
            bandService.putToDirtyBandList(dataBand);
            bandService.putToDirtyBandList(metaBand);
        }
        return true;
    }

    public boolean delete(SortListObject sortObj) throws Exception {
        boolean rb = false;
        synchronized (this) {
            synchronized (((SortBandListFactory) bandService).getGlobalLocker()) {
                bandService.writeLogAndDuplicate(OP_DELETE + "," + this.listId + "," + sortObj.getObjid() + "," + sortObj.getKey() + "," + sortObj.getTxid() + "\n", sortObj.getTxid());
                rb = internal_delete(sortObj);
            }
        }

        return rb;
    }

    synchronized boolean internal_add(SortListObject sortObj) throws Exception {
        size++;
        Band hband = getHeadBand();
        if (hband.getMeta() == 0) {
            if (hband.getObjList().size() == bandService.getMaxObjectsPerBand()) {
                switchToMetaMode();
                hband.setTxid(sortObj.getTxid());
                hband = getHeadBand();
            }
        }
        if (hband.getMeta() == 0) {
            if (hband.getObjList().size() == 0) {
                hband.getObjList().add(sortObj);
                hband.setTxid(sortObj.getTxid());
                bandService.putToDirtyBandList(hband);
                return true;
            }
            int index = Collections.binarySearch(hband.getObjList(), sortObj, ascComparator);
            if (index >= 0) {
                size--;
                logger.log(Level.FINE, "add :key=" + sortObj.getKey() + ",objid=" + sortObj.getObjid() + ",listId=" + listId + " already existss");
                return false;
            } else {
                int insertPoint = -(index + 1);
                hband.getObjList().add(insertPoint, sortObj);
                hband.setTxid(sortObj.getTxid());
                bandService.putToDirtyBandList(hband);
                return true;
            }
        } else {
            Pos pos = search(sortObj);
            int metaBandInsertPos;
            int bandInsertIndex;
            int dataIndex;
            if (pos.metaBandIndex >= 0 && pos.bandInfoIndex >= 0 && pos.dataIndex >= 0) {
                size--;
                logger.log(Level.FINE, "add :key=" + sortObj.getKey() + ",objid=" + sortObj.getObjid() + ",listId=" + listId + " already existss");
                return false;
            }
            if (pos.metaBandIndex < 0) {
                metaBandInsertPos = -(pos.metaBandIndex + 1);
                if (metaBandInsertPos >= metaBands.size()) {
                    metaBandInsertPos = metaBands.size() - 1;
                    Band metaBand = metaBands.get(metaBandInsertPos);
                    bandInsertIndex = metaBand.getBandInfos().size() - 1;
                    Band.BandInfo bandInfo = metaBand.getBandInfos().get(bandInsertIndex);
                    dataIndex = (int) bandInfo.getNumber();
                } else {
                    bandInsertIndex = 0;
                    dataIndex = 0;
                }
            } else {
                metaBandInsertPos = pos.metaBandIndex;
                if (pos.bandInfoIndex < 0) {
                    bandInsertIndex = -(pos.bandInfoIndex + 1);
                    Band metaBand = metaBands.get(metaBandInsertPos);
                    if (bandInsertIndex >= metaBand.getBandInfos().size()) {
                        bandInsertIndex = metaBand.getBandInfos().size() - 1;
                        dataIndex = (int) metaBand.getBandInfos().get(bandInsertIndex).getNumber();
                    } else {
                        dataIndex = 0;
                    }
                } else {
                    bandInsertIndex = pos.bandInfoIndex;
                    if (pos.dataIndex < 0) {
                        dataIndex = -(pos.dataIndex + 1);
                    } else {
                        dataIndex = pos.dataIndex;
                    }
                }
            }
            add(metaBandInsertPos, bandInsertIndex, dataIndex, sortObj);
        }
        return true;

    }

    boolean debugMode = false;

    public boolean isDebugMode() {
        return debugMode;
    }

    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }

    public boolean checkInternalState() throws Exception {
        if (!debugMode) {
            return true;
        }
        if (!checkSize()) {
            return false;
        }
        Band hband = getHeadBand();
        if (hband.getMeta() == 0) {
            return true;
        }
        for (int i = 0; i < metaBands.size(); i++) {
            Band metaBand = metaBands.get(i);
            List<Band.BandInfo> infos = metaBand.getBandInfos();
            String lastMax = "";
            for (int j = 0; j < infos.size(); j++) {
                Band.BandInfo info = infos.get(j);
                Band dataBand = bandService.getBandById(this, info.getBandId());
                if (dataBand.getObjList().size() != info.getNumber()) {
                    return false;
                }
                this.updateKeyRange(dataBand);
                if (!dataBand.getMaxKey().equals(info.getMaxKey())) {
                    return false;
                }
                if (!dataBand.getMinKey().equals(info.getMinKey())) {
                    return false;
                }
                if (info.getMinKey().compareTo(lastMax) < 0) {
                    return false;
                }
                if (info.getMinKey().compareTo(info.getMaxKey()) > 0) {
                    return false;
                }
                lastMax = info.getMaxKey();
            }
        }
        return true;
    }

    public boolean checkSize() throws Exception {
        if (debugMode) {
            Band hband = getHeadBand();
            if (hband.getMeta() == 0) {
                if (size != hband.getObjList().size()) {
                    return false;
                } else {
                    return true;
                }
            } else {
                int total = 0;
                for (int i = 0; i < metaBands.size(); i++) {
                    Band metaBand = metaBands.get(i);
                    List<Band.BandInfo> infos = metaBand.getBandInfos();
                    for (int j = 0; j < infos.size(); j++) {
                        Band.BandInfo info = infos.get(j);
                        total += info.getNumber();
                    }
                }
                if (total != size) {
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            return true;
        }
    }

    public boolean add(SortListObject sortObj) throws Exception {
        boolean rb = false;
        synchronized (this) {
            synchronized (((SortBandListFactory) bandService).getGlobalLocker()) {
                long ver = bandService.writeLogAndDuplicate(OP_ADD + "," + this.listId + "," + sortObj.getObjid() + "," + sortObj.getKey() + "," + sortObj.getTxid() + "\n", sortObj.getTxid());
                rb = internal_add(sortObj);
            }
        }

        return rb;
    }

    public boolean add(List<SortListObject> listSortObj) throws Exception {
        for (SortListObject sortObj : listSortObj) {
            if (!add(sortObj)) {
                // throw new Exception("batchAdd error, key = " + sortObj.getKey() + ", obj = " + sortObj.getObjid());
                System.out.println("batchAdd [ok dup], key = " + sortObj.getKey() + ", obj = " + sortObj.getObjid());
            }
        }
        return true;
    }

    private void adjustBandInfo(Band.BandInfo bandInfo, Band dataBand) {
        SortListObject minObj, maxObj;
        minObj = (SortListObject) dataBand.getObjList().get(0);
        maxObj = (SortListObject) dataBand.getObjList().get(dataBand.getObjList().size() - 1);
        bandInfo.setMinKey(minObj.getKey());
        bandInfo.setMaxKey(maxObj.getKey());
        bandInfo.setNumber(dataBand.getObjList().size());
    }

    String makeKey(SortListObject obj) {
        return obj.getWholeKey();
    }

    void updateKeyRange(Band dataBand) throws Exception {
        SortListObject minObj = (SortListObject) dataBand.getObjList().get(0);
        SortListObject maxObj = (SortListObject) dataBand.getObjList().get(dataBand.getObjList().size() - 1);
        dataBand.setMinKey(makeKey(minObj));
        dataBand.setMaxKey(makeKey(maxObj));
    }

    Band.BandInfo getBandInfo(Band dataBand) throws Exception {
        updateKeyRange(dataBand);
        Band.BandInfo info = new Band.BandInfo();
        info.setMinKey(dataBand.getMinKey());
        info.setMaxKey(dataBand.getMaxKey());
        info.setNumber(dataBand.getObjList().size());
        info.setBandId(dataBand.getId());
        return info;
    }

    void updateBandInfo(Band dataBand, Band.BandInfo info) throws Exception {
        updateKeyRange(dataBand);
        info.setMinKey(dataBand.getMinKey());
        info.setMaxKey(dataBand.getMaxKey());
        info.setNumber(dataBand.getObjList().size());
        info.setBandId(dataBand.getId());
    }

    List<Band> addObjectToBand(Band dataBand, int dataIndex, SortListObject sortObj) throws Exception {
        Vector result = new Vector();
        if (!this.isDataBandFull(dataBand)) {
            if (dataIndex >= dataBand.getObjList().size()) {
                dataBand.getObjList().add(sortObj);
            } else {
                try {
                    dataBand.getObjList().add(dataIndex, sortObj);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            updateKeyRange(dataBand);
            result.add(dataBand);
            return result;
        } else {
            Band newDataband = new Band();
            newDataband.setListName(this.listId);
            newDataband.setMeta(0);
            newDataband.setHead(0);
            newDataband.setId(bandService.getNewBandId());
            List origDataList = dataBand.getObjList();
            List oldDataList = new Vector();
            List newDataList = new Vector();
            origDataList.add(dataIndex, sortObj);
            oldDataList.addAll(origDataList.subList(0, origDataList.size() / 2));
            newDataList.addAll(origDataList.subList(oldDataList.size(), origDataList.size()));
            dataBand.setObjList(oldDataList);
            newDataband.setObjList(newDataList);
            updateKeyRange(dataBand);
            updateKeyRange(newDataband);
            result.add(dataBand);
            result.add(newDataband);
            return result;
        }
    }

    List<Band.BandInfo> adjustBandInfos(Band.BandInfo bandInfo, List<Band> bands) throws Exception {
        Vector result = new Vector();
        if (bands.size() == 1) {
            updateBandInfo(bands.get(0), bandInfo);
            result.add(bandInfo);
            return result;
        } else {
            updateBandInfo(bands.get(0), bandInfo);
            result.add(bandInfo);
            result.add(getBandInfo(bands.get(1)));
            return result;
        }
    }

    static void updateMetaBandRange(Band metaBand) {
        if (metaBand.getBandInfos().size() > 0) {
            metaBand.setMinKey(metaBand.getBandInfos().get(0).getMinKey());
            metaBand.setMaxKey(metaBand.getBandInfos().get(metaBand.getBandInfos().size() - 1).getMaxKey());
        }
    }

    List<Band> updateMetaBand(Band metaBand, int bandInsertIndex, List<Band.BandInfo> infos) throws Exception {
        Vector result = new Vector();
        if (infos.size() == 1) {
            updateMetaBandRange(metaBand);
            result.add(metaBand);
            return result;
        } else {
            if (!this.isMetaBandFull(metaBand)) {
                metaBand.getBandInfos().add(bandInsertIndex + 1, infos.get(1));
                updateMetaBandRange(metaBand);
                result.add(metaBand);
                return result;
            } else {
                Band newMetaBand = new Band();
                newMetaBand.setId(bandService.getNewBandId());
                newMetaBand.setListName(this.listId);
                newMetaBand.setHead(0);
                newMetaBand.setMeta(1);
                List<Band.BandInfo> origBandInfos = metaBand.getBandInfos();
                origBandInfos.add(bandInsertIndex + 1, infos.get(1));
                Vector<Band.BandInfo> oldBandInfos = new Vector<Band.BandInfo>();
                Vector<Band.BandInfo> newBandInfos = new Vector<Band.BandInfo>();
                oldBandInfos.addAll(origBandInfos.subList(0, origBandInfos.size() / 2));
                newBandInfos.addAll(origBandInfos.subList(oldBandInfos.size(), origBandInfos.size()));
                metaBand.setBandInfos(oldBandInfos);
                newMetaBand.setBandInfos(newBandInfos);
                updateMetaBandRange(metaBand);
                updateMetaBandRange(newMetaBand);
                result.add(metaBand);
                result.add(newMetaBand);
                return result;
            }
        }
    }

    private void add(int metaBandInsertPos, int bandInsertIndex, int dataIndex, SortListObject sortObj) throws Exception {
        if (!this.checkSearchResult(metaBandInsertPos, bandInsertIndex, dataIndex, sortObj)) {
            throw new Exception("Search error.");
        }
        Band metaBand;
        metaBand = metaBands.get(metaBandInsertPos);
        metaBands.get(metaBandInsertPos);
        Band.BandInfo bandInfo = metaBand.getBandInfos().get(bandInsertIndex);
        Band dataBand = bandService.getBandById(this, bandInfo.getBandId());
        List<Band> newBands = this.addObjectToBand(dataBand, dataIndex, sortObj);
        List<Band.BandInfo> infos = this.adjustBandInfos(bandInfo, newBands);
        List<Band> metaBands = this.updateMetaBand(metaBand, bandInsertIndex, infos);
        if (metaBands.size() == 2) {
            metaBands.get(0).setTxid(sortObj.getTxid());
            metaBands.get(1).setTxid(sortObj.getTxid());
            this.addMetaBandAfter(metaBands.get(0), metaBands.get(1));
        } else {
            assert (metaBands.get(0) == metaBand);
            metaBands.get(0).setTxid(sortObj.getTxid());
            bandService.putToDirtyBandList(metaBands.get(0));
        }
        for (Band band : newBands) {
            band.setTxid(sortObj.getTxid());
            bandService.putToDirtyBandList(band);
        }
    }

    private void addMetaBandAfter(Band oldBand, Band newBand) throws Exception {
        newBand.setNextMetaBandId(oldBand.getNextMetaBandId());
        newBand.setPrevMetaBandId(oldBand.getId());
        oldBand.setNextMetaBandId(newBand.getId());
        int idx = metaBands.lastIndexOf(oldBand);
        if (idx < metaBands.size() - 1) {
            Band oldNextBand = metaBands.get(idx + 1);
            oldNextBand.setPrevMetaBandId(newBand.getId());
            bandService.putToDirtyBandList(oldNextBand);
        }
        metaBands.add(idx + 1, newBand);
        bandService.putToDirtyBandList(newBand);
        bandService.putToDirtyBandList(oldBand);
    }

    boolean checkDataBand(Band dataBand) {
        if (debugMode) {
            String max = "";
            for (int i = 0; i < dataBand.getObjList().size(); i++) {
                SortListObject sobj = (SortListObject) dataBand.getObjList().get(i);
                if (max.compareTo(sobj.getKey()) > 0) {
                    return false;
                }
                max = sobj.getKey();
            }
        }
        return true;
    }

    private void switchToMetaMode() throws Exception {
        Band hband = getHeadBand();
        Band metaBand = new Band();
        metaBand.setListName(this.listId);
        metaBand.setMeta(1);
        metaBand.setHead(1);
        Band.BandInfo bandInfo = getBandInfo(hband);
        metaBand.getBandInfos().add(bandInfo);
        updateMetaBandRange(metaBand);
        metaBand.setId(bandService.getNewBandId());
        this.headBand = metaBand;
        hband.setHead(0);
        if (metaBands == null) {
            metaBands = new Vector();
        }
        metaBands.add(metaBand);
        bandService.putToDirtyBandList(hband);
        bandService.putToDirtyBandList(metaBand);
        checkDataBand(hband);
    }

    synchronized boolean internal_reorder(SortListObject oldObj, SortListObject newObj) throws Exception {
        internal_delete(oldObj);
        internal_add(newObj);
        return true;
    }

    public boolean reorder(SortListObject oldObj, SortListObject newObj) throws Exception {
        boolean rb = false;
        synchronized (this) {
            synchronized (((SortBandListFactory) bandService).getGlobalLocker()) {
                bandService.writeLogAndDuplicate(OP_REORDER + "," + this.listId + "," + oldObj.getObjid() + "," + oldObj.getKey() + "," + newObj.getObjid() + "," + newObj.getKey() + "," + newObj.getTxid() + "\n", newObj.getTxid());
                rb = internal_reorder(oldObj, newObj);
            }
        }
        return rb;
    }

    public long getLessOrEqualPos(SortListObject obj) throws Exception {
        if (obj == null) {
            return 0;
        }
        Band hband = getHeadBand();
        if (hband.getMeta() == 0) {
            if (hband.getObjList().size() == 0) {
                return 0;
            }
            int index = Collections.binarySearch(hband.getObjList(), obj, ascComparator);
            if (index >= 0) {
                return index;
            } else {
                int insertPoint = -(index + 1);
                return insertPoint;
            }
        }
        Pos pos = search(obj);
        int metaBandInsertPos;
        int bandInsertIndex;
        int dataIndex;
        if (pos.metaBandIndex < 0) {
            metaBandInsertPos = -(pos.metaBandIndex + 1);
            if (metaBandInsertPos >= metaBands.size()) {
                return size;
            } else {
                bandInsertIndex = 0;
                dataIndex = 0;
            }
        } else {
            metaBandInsertPos = pos.metaBandIndex;
            if (pos.bandInfoIndex < 0) {
                bandInsertIndex = -(pos.bandInfoIndex + 1);
                Band metaBand = metaBands.get(metaBandInsertPos);
                if (bandInsertIndex >= metaBand.getBandInfos().size()) {
                    bandInsertIndex = metaBand.getBandInfos().size() - 1;
                    dataIndex = (int) metaBand.getBandInfos().get(bandInsertIndex).getNumber();
                } else {
                    dataIndex = 0;
                }
            } else {
                bandInsertIndex = pos.bandInfoIndex;
                if (pos.dataIndex < 0) {
                    dataIndex = -(pos.dataIndex + 1);
                } else {
                    dataIndex = pos.dataIndex;
                }
            }
        }
        int curIndex = 0;
        boolean found = false;
        int bandInfoIndex;
        for (int i = 0; i <= metaBandInsertPos; i++) {
            Band metaBand = this.metaBands.get(i);
            bandInfoIndex = 0;
            for (Band.BandInfo bandInfo : metaBand.getBandInfos()) {
                if (bandInfoIndex == bandInsertIndex && i == metaBandInsertPos) {
                    int num = dataIndex > bandInfo.getNumber() ? (int) bandInfo.getNumber() : dataIndex;
                    curIndex += num;
                    break;
                } else {
                    curIndex += bandInfo.getNumber();
                }
                bandInfoIndex++;
            }
        }
        return curIndex;
    }

    synchronized public SortListObject getSortListObject(String key) throws Exception {
        Band hband = getHeadBand();
        SortListObject obj = new SortListObject();
        obj.setKey(key);
        obj.setObjid("");
        if (hband.getMeta() == 0) {
            if (hband.getObjList().size() == 0) {
                return null;
            }
            int index = Collections.binarySearch(hband.getObjList(), obj, ascComparator);
            if (index >= 0) {
                hband.getObjList().get(index);
            } else {
                index = -index - 1;
                if (index >= hband.getObjList().size()) {
                    return null;
                } else {
                    SortListObject ret = (SortListObject) hband.getObjList().get(index);
                    if (StringUtils.equals(ret.getKey(), key)) {
                        return ret;
                    } else {
                        return null;
                    }
                }
            }
        }
        Pos pos = search(obj);
        int metaBandInsertPos;
        int bandInsertIndex;
        int dataIndex;
        if (pos.metaBandIndex < 0) {
            metaBandInsertPos = -(pos.metaBandIndex + 1);
            if (metaBandInsertPos >= metaBands.size()) {
                return null;
            } else {
                bandInsertIndex = 0;
                dataIndex = 0;
            }
        } else {
            metaBandInsertPos = pos.metaBandIndex;
            if (pos.bandInfoIndex < 0) {
                bandInsertIndex = -(pos.bandInfoIndex + 1);
                Band metaBand = metaBands.get(metaBandInsertPos);
                if (bandInsertIndex >= metaBand.getBandInfos().size()) {
                    bandInsertIndex = metaBand.getBandInfos().size() - 1;
                    dataIndex = (int) metaBand.getBandInfos().get(bandInsertIndex).getNumber();
                } else {
                    dataIndex = 0;
                }
            } else {
                bandInsertIndex = pos.bandInfoIndex;
                if (pos.dataIndex < 0) {
                    dataIndex = -(pos.dataIndex + 1);
                } else {
                    dataIndex = pos.dataIndex;
                }
            }
        }
        Band metaBand = this.metaBands.get(metaBandInsertPos);
        Band.BandInfo binfo = metaBand.getBandInfos().get(bandInsertIndex);
        Band b = bandService.getBandById(this, binfo.getBandId());
        if (dataIndex >= b.getObjList().size()) {
            return null;
        }
        SortListObject ret = (SortListObject) b.getObjList().get(dataIndex);
        if (StringUtils.equals(ret.getKey(), key)) {
            return ret;
        } else {
            return null;
        }
    }

    public List<SortListObject> getHigherOrEqual(SortListObject obj, int num) throws Exception {
        int pos = (int) this.getLessOrEqualPos(obj);
        if (pos < 0) {
            return null;
        }
        if (pos >= this.getSize()) {
            return null;
        }
        int n = num + 1;
        if (n + pos > this.getSize()) {
            n = (int) this.getSize() - pos;
        }
        List<SortListObject> objs = this.getRange(pos, n);
        return objs;
    }

    public boolean isExists(String key, String objid) throws Exception {
        SortListObject so = new SortListObject();
        so.setKey(key);
        so.setObjid(objid);
        Pos pos = search(so);
        if (pos.metaBandIndex >= 0 && pos.bandInfoIndex >= 0 && pos.dataIndex >= 0) {
            return true;
        } else {
            return false;
        }
    }

    private class Pos {
        boolean found;
        int index;
        int metaBandIndex;
        int bandInfoIndex;
        Band dataBand;
        int dataIndex;
    }

    private static class AscComparator implements Comparator<SortListObject> {
        public int compare(SortListObject o1, SortListObject o2) {
            int r = o1.getKey().compareTo(o2.getKey());
            if (r == 0) {
                int r1 = o1.getObjid().compareTo(o2.getObjid());
                return r1;
            } else {
                return r;
            }
        }
    }

    private static AscComparator ascComparator = new AscComparator();

    static MetaBandSearchComparator metaBandSearchComparator = new MetaBandSearchComparator();

    private static class MetaBandSearchComparator implements Comparator<Band> {
        public int compare(Band o1, Band o2) {
            if (o1 == null || o2 == null) {
                System.out.println("null pointer exception");
            }
            if (o1.getMaxKey() == null || o1.getMinKey() == null) {
                updateMetaBandRange(o1);
            }
            if (o2.getMaxKey() == null || o2.getMinKey() == null) {
                updateMetaBandRange(o2);
            }
            String[] minParts1 = o1.getMinKey().split(SortListObject.KEY_DELIMITER);
            String[] maxParts1 = o1.getMaxKey().split(SortListObject.KEY_DELIMITER);
            String[] minParts2 = o2.getMinKey().split(SortListObject.KEY_DELIMITER);
            String[] maxParts2 = o2.getMaxKey().split(SortListObject.KEY_DELIMITER);
            SortListObject obj1Min = null;
            SortListObject obj1Max = null;
            SortListObject obj2Min = null;
            SortListObject obj2Max = null;
            if (minParts1.length > 1) {
                obj1Min = new SortListObject(minParts1[0], minParts1[1]);
            } else if (minParts1.length > 0) {
                obj1Min = new SortListObject(minParts1[0], "");
            } else {
                obj1Min = new SortListObject("", "");
            }
            if (maxParts1.length > 1) {
                obj1Max = new SortListObject(maxParts1[0], maxParts1[1]);
            } else if (maxParts1.length > 0) {
                obj1Max = new SortListObject(maxParts1[0], "");
            } else {
                obj1Max = new SortListObject("", "");
            }
            if (minParts2.length > 1) {
                obj2Min = new SortListObject(minParts2[0], minParts2[1]);
            } else if (minParts2.length > 0) {
                obj2Min = new SortListObject(minParts2[0], "");
            } else {
                obj2Min = new SortListObject("", "");
            }
            if (maxParts2.length > 1) {
                obj2Max = new SortListObject(maxParts2[0], maxParts2[1]);
            } else if (maxParts2.length > 0) {
                obj2Max = new SortListObject(maxParts2[0], "");
            } else {
                obj2Max = new SortListObject("", "");
            }
            if (obj2Min.compareTo(obj2Max) == 0) {
                return CommonTools.compareRangeAndPoint(obj1Min, obj1Max, obj2Min);
            } else {
                return CommonTools.compareRangeAndPoint(obj2Min, obj2Max, obj1Min);
            }
        }
    }

    static BandInfoSearchComparator bandInfoSearchComparator = new BandInfoSearchComparator();

    private static class BandInfoSearchComparator implements Comparator<Band.BandInfo> {
        public int compare(Band.BandInfo o1, Band.BandInfo o2) {
            SortListObject obj1Min = new SortListObject(o1.getMinKey());
            SortListObject obj1Max = new SortListObject(o1.getMaxKey());
            SortListObject obj2Min = new SortListObject(o2.getMinKey());
            SortListObject obj2Max = new SortListObject(o2.getMaxKey());
            if (obj2Min.compareTo(obj2Max) == 0) {
                return CommonTools.compareRangeAndPoint(obj1Min, obj1Max, obj2Min);
            } else {
                return CommonTools.compareRangeAndPoint(obj2Min, obj2Max, obj1Min);
            }
        }
    }

    private Band getHeadBand() throws Exception {
        if (this.headBand == null) {
            headBand = bandService.getHeadBand(this.listId);
        }
        if (headBand == null) {
            headBand = new Band();
            headBand.setListName(this.getListId());
            headBand.setHead(1);
            headBand.setMeta(0);
            headBand.setId(bandService.getNewBandId());
        }
        return headBand;
    }

    private int binarySearch(List<SortListObject> objs, SortListObject obj) {
        int pos = Collections.binarySearch(objs, obj, ascComparator);
        return pos;
    }

    private Pos search(SortListObject obj) throws Exception {
        Band hband = getHeadBand();
        if (hband.getMeta() == 0) {
            int idx = binarySearch(hband.getObjList(), obj);
            if (idx >= 0) {
                Pos pos = new Pos();
                pos.index = idx;
                pos.metaBandIndex = 0;
                pos.dataBand = hband;
                pos.bandInfoIndex = 0;
                pos.dataIndex = idx;
                pos.found = true;
                return pos;
            } else {
                Pos pos = new Pos();
                pos.index = idx;
                pos.metaBandIndex = 0;
                pos.dataBand = hband;
                pos.bandInfoIndex = 0;
                pos.dataIndex = idx;
                pos.found = false;
                return pos;
            }
        } else {
            Band searchMetaBand = new Band();
            String key = makeKey(obj);
            searchMetaBand.setMinKey(key);
            searchMetaBand.setMaxKey(key);
            int metaIndex = Collections.binarySearch(metaBands, searchMetaBand, metaBandSearchComparator);
            if (metaIndex < 0) {
                Pos pos = new Pos();
                pos.index = -1;
                pos.metaBandIndex = metaIndex;
                pos.dataBand = null;
                pos.bandInfoIndex = -1;
                pos.dataIndex = -1;
                pos.found = false;
                return pos;
            }
            Band metaband = metaBands.get(metaIndex);
            Band.BandInfo searchBandInfo = new Band.BandInfo();
            String bkey = makeKey(obj);
            searchBandInfo.setMinKey(bkey);
            searchBandInfo.setMaxKey(bkey);
            int bandIndex = Collections.binarySearch(metaband.getBandInfos(), searchBandInfo, bandInfoSearchComparator);
            if (bandIndex < 0) {
                Pos pos = new Pos();
                pos.index = -1;
                pos.metaBandIndex = metaIndex;
                pos.dataBand = null;
                pos.bandInfoIndex = bandIndex;
                pos.dataIndex = -1;
                pos.found = false;
                return pos;
            }
            Band.BandInfo bandinfo = metaband.getBandInfos().get(bandIndex);
            Band dataBand = bandService.getBandById(this, bandinfo.getBandId());
            int dataIndex = binarySearch(dataBand.getObjList(), obj);
            if (dataIndex >= 0) {
                Pos pos = new Pos();
                pos.index = -1;
                pos.metaBandIndex = metaIndex;
                pos.dataBand = dataBand;
                pos.bandInfoIndex = bandIndex;
                pos.dataIndex = dataIndex;
                pos.found = true;
                return pos;
            }
            Pos pos = new Pos();
            pos.found = false;
            pos.index = -1;
            pos.metaBandIndex = metaIndex;
            pos.bandInfoIndex = bandIndex;
            pos.dataBand = dataBand;
            pos.dataIndex = dataIndex;
            return pos;
        }
    }

    boolean checkSearchResult(int insertMetaIndex, int insertBandIndex, int insertDataIndex, SortListObject sobj) throws Exception {
        if (!debugMode) {
            return true;
        }
        String key = sobj.getKey();
        if (insertMetaIndex >= metaBands.size()) {
            return false;
        }
        Band metaBand = metaBands.get(insertMetaIndex);
        if (key.compareTo(metaBand.getMinKey()) < 0) {
            if (insertBandIndex != 0) {
                return false;
            }
            if (insertDataIndex != 0) {
                return false;
            }
        }
        if (key.compareTo(metaBand.getMaxKey()) > 0) {
            if (insertMetaIndex != metaBands.size() - 1) {
                return false;
            }
            if (metaBand.getBandInfos().size() - 1 != insertBandIndex) {
                return false;
            }
        }
        Band.BandInfo info = metaBand.getBandInfos().get(insertBandIndex);
        if (key.compareTo(info.getMinKey()) < 0) {
            if (insertDataIndex != 0) {
                return false;
            }
        }
        if (key.compareTo(info.getMaxKey()) > 0) {
            if (insertDataIndex < info.getNumber()) {
                return false;
            }
        }
        Band dataBand = bandService.getBandById(this, info.getBandId());
        if (dataBand == null) {
            return false;
        }
        if (insertDataIndex == 0) {
            if (key.compareTo(info.getMinKey()) > 0) {
                return false;
            }
        } else {
            SortListObject preObj = (SortListObject) dataBand.getObjList().get(insertDataIndex - 1);
            if (preObj.getKey().compareTo(key) > 0) {
                return false;
            }
        }
        if (insertDataIndex < dataBand.getObjList().size()) {
            SortListObject nextObj = (SortListObject) dataBand.getObjList().get(insertDataIndex);
            if (nextObj.getKey().compareTo(key) < 0) {
                return false;
            }
        }
        return true;
    }

    private boolean isDataBandFull(Band band) throws Exception {
        return band.getObjList().size() >= bandService.getMaxObjectsPerBand();
    }

    private boolean isMetaBandFull(Band band) throws Exception {
        return band.getBandInfos().size() >= bandService.getMaxBandInfosPerBand();
    }

    private void removeMetaBand(Band metaBand, int metaBandIndex) throws Exception {
        Band prevBand = null;
        Band nextBand = null;
        if (metaBandIndex > 0) {
            prevBand = metaBands.get(metaBandIndex - 1);
            if (prevBand != null) {
                prevBand.setNextMetaBandId(metaBand.getNextMetaBandId());
            }
        }
        if (metaBand.getNextMetaBandId() > 0) {
            nextBand = metaBands.get(metaBandIndex + 1);
            if (nextBand != null) {
                nextBand.setPrevMetaBandId(metaBand.getPrevMetaBandId());
            }
            if (nextBand.getPrevMetaBandId() == 0) {
                nextBand.setHead(1);
                this.headBand = nextBand;
            }
        }
        metaBands.remove(metaBand);
        if (metaBand.getHead() == 1 && size == 0) {
            metaBand.getObjList().clear();
            metaBand.setHead(0);
            metaBand.setMeta(0);
        }
        if (prevBand != null) {
            prevBand.setTxid(metaBand.getTxid());
            bandService.putToDirtyBandList(prevBand);
        }
        if (nextBand != null) {
            nextBand.setTxid(metaBand.getTxid());
            bandService.putToDirtyBandList(nextBand);
        }
        bandService.putToDirtyBandList(metaBand);
    }

}

