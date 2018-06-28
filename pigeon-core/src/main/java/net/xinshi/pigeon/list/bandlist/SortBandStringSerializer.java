package net.xinshi.pigeon.list.bandlist;

import net.xinshi.pigeon.list.SortListObject;
import net.xinshi.pigeon.list.bandlist.bean.Band;

import java.util.Collection;
import java.util.List;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-11-29
 * Time: 8:48:37
 * To change this template use File | Settings | File Templates.
 */

public class SortBandStringSerializer implements IBandSerializer {

    final static String BAND_SEPERATOR = "\n";

    public Object serialize(Collection<Band> bands) throws Exception {
        if (bands == null) {
            return null;
        }
        if (bands.size() == 0) {
            return null;
        }
        StringBuilder strBuilder = new StringBuilder();
        for (Band band : bands) {
            if (band.getMeta() == 0) {
                strBuilder.append(band.getId()).append(",").append(band.getMeta()).append(",").append(band.getHead());
                strBuilder.append(",").append(band.getListName()).append(";");
                for (SortListObject sortObj : (List<SortListObject>) band.getObjList()) {
                    strBuilder.append(sortObj.getObjid()).append(",").append(sortObj.getKey()).append(";");
                }
                strBuilder.append(BAND_SEPERATOR);
            } else {
                strBuilder.append(band.getId()).append(",").append(band.getMeta()).append(",").append(band.getHead());
                strBuilder.append(",").append(band.getListName()).append(",").append(band.getPrevMetaBandId()).append(",").append(band.getNextMetaBandId()).append(";");
                for (Band.BandInfo info : band.getBandInfos()) {
                    strBuilder.append(info.getBandId()).append(",").append(info.getNumber()).append(",");
                    strBuilder.append(info.getMinKey()).append(",").append(info.getMaxKey()).append(";");
                }
                strBuilder.append(BAND_SEPERATOR);
            }
        }
        return strBuilder.toString();
    }

    public List<Band> unserializeBandList(Object input) throws Exception {
        throw new Exception("not implemented.");
    }

    public Band unserializeBand(Object input) throws Exception {
        Band result = new Band();
        String s = (String) input;
        int curPos = s.indexOf(";");
        if (curPos <= 0) {
            return null;
        }
        String h;
        h = s.substring(0, curPos);
        String value = s.substring(curPos + 1);
        String[] fields = h.split(",");
        result.setId(new Integer(fields[0]));
        result.setMeta(new Integer(fields[1]));
        result.setHead(new Integer(fields[2]));
        result.setListName(fields[3]);
        if (result.getMeta() == 1) {
            result.setPrevMetaBandId(new Long(fields[4]));
            result.setNextMetaBandId(new Long(fields[5]));
        }
        result.setValue(value);
        return result;
    }

    public void parseValue(Band band) throws Exception {
        if (band.getMeta() == 0) {
            String[] strObjs = band.getValue().split(";");
            List<SortListObject> objs = new Vector<SortListObject>();
            for (String strObj : strObjs) {
                String[] fields = strObj.split(",");
                if (fields.length == 2) {
                    SortListObject obj = new SortListObject();
                    obj.setObjid(fields[0]);
                    obj.setKey(fields[1]);
                    objs.add(obj);
                }
            }
            band.setObjList(objs);
        } else if (band.getMeta() == 1) {
            String[] strInfos = band.getValue().split(";");
            List<Band.BandInfo> infos = new Vector<Band.BandInfo>();
            for (String strInfo : strInfos) {
                String[] fields = strInfo.split(",");
                if (fields.length == 4) {
                    Band.BandInfo info = new Band.BandInfo();
                    info.setBandId(new Long(fields[0]));
                    info.setNumber(new Long(fields[1]));
                    info.setMinKey(fields[2]);
                    info.setMaxKey(fields[3]);
                    infos.add(info);
                }
            }
            band.setBandInfos(infos);
            band.setMinKey(infos.get(0).getMinKey());
            band.setMaxKey(infos.get(infos.size() - 1).getMaxKey());
        } else {
            throw new Exception("");
        }
    }

}

