package net.xinshi.pigeon.util;


import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2010-1-30
 * Time: 11:40:56
 * To change this template use File | Settings | File Templates.
 */
public class CommonTools {
    static final byte compressFlag = 0x01;
    static final byte stringFlag = 0x02;
    static final byte addFlag = 1 << 3;
    static Logger logger = Logger.getLogger("CommonTools");

    public static String getComparableString(long key, int number) {
        return StringUtils.leftPad(String.valueOf(key), number, '0');
    }

    static long pow(long key, int power) {
        long powers[] = new long[]{1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L, 1000000000L, 10000000000L, 100000000000L, 1000000000000L,10000000000000L,100000000000000L};
        return key * powers[power];
    }

    public static String getRevertComparableString(long key, int number) {
        long rkey = pow(1, number) - key;
        return getComparableString(rkey, number);
    }

    public static long getRevertedNum(String key, int number) {
        long lkey = Long.parseLong(key);
        return pow(1, number) - lkey;


    }

    public static void copyObject(JSONObject to, JSONObject from) throws Exception {
        for (Iterator it = from.keys(); it.hasNext();) {
            String key = (String) it.next();
            to.put(key, from.opt(key));
        }
    }

    static public JSONObject getObject(String objId, IFlexObjectFactory flexObjectFactory) throws Exception {
        String content = flexObjectFactory.getContent(objId);
        if (StringUtils.isBlank(content)) {
            return null;
        } else {
            return new JSONObject(content);
        }
    }


    static public List<JSONObject> getObjects(List<String> objIds, IFlexObjectFactory flexObjectFactory) throws Exception {
        List<String> contents = flexObjectFactory.getContents(objIds);
        Vector<JSONObject> result = new Vector();
        for (String content : contents) {
            if (StringUtils.isBlank(content)) {
                result.add(null);
            } else {
                result.add(new JSONObject(content));
            }
        }
        return result;

    }


    public static byte[] int2bytes(int len) {
        byte[] lenbuf = new byte[4];
        lenbuf[0] = (byte) (len & 0x000000ff);
        lenbuf[1] = (byte) ((len >> 8) & 0x000000ff);
        lenbuf[2] = (byte) ((len >> 16) & 0x000000ff);
        lenbuf[3] = (byte) ((len >> 24) & 0x000000ff);
        return lenbuf;
    }

    public static int bytes2int(byte[] bytes) {
        long l;
        l = bytes[3] & 0xFF;
        l = l << 8;
        l |= bytes[2] & 0xFF;
        l <<= 8;
        l |= bytes[1] & 0xFF;
        l <<= 8;
        l |= bytes[0] & 0xFF;
        return (int) l;
    }

    public static int bytes2int(byte[] bytes, int beginPos) {
        long l;
        l = bytes[beginPos + 3] & 0xFF;
        l = l << 8;
        l |= bytes[beginPos + 2] & 0xFF;
        l <<= 8;
        l |= bytes[beginPos + 1] & 0xFF;
        l <<= 8;
        l |= bytes[beginPos + 0] & 0xFF;
        return (int) l;
    }

    public static int bytes2intJAVA(byte[] array) {
        return (array[0] & 0xff) << 24 |
                (array[1] & 0xff) << 16 |
                (array[2] & 0xff) << 8 |
                (array[3] & 0xff) << 0;

    }

    public static int bytes2intJAVA(byte[] array, int index) {
        return (array[index] & 0xff) << 24 |
                (array[index + 1] & 0xff) << 16 |
                (array[index + 2] & 0xff) << 8 |
                (array[index + 3] & 0xff) << 0;
    }

    public static long bytes2longJAVA(byte[] writeBuffer, int offset) {
        long v = 0;
        v |= ((long) writeBuffer[0 + offset] & 0xFF) << 56;
        v |= ((long) writeBuffer[1 + offset] & 0xFF) << 48;
        v |= ((long) writeBuffer[2 + offset] & 0xFF) << 40;
        v |= ((long) writeBuffer[3 + offset] & 0xFF) << 32;
        v |= ((long) writeBuffer[4 + offset] & 0xFF) << 24;
        v |= ((long) writeBuffer[5 + offset] & 0xFF) << 16;
        v |= ((long) writeBuffer[6 + offset] & 0xFF) << 8;
        v |= ((long) writeBuffer[7 + offset] & 0xFF) << 0;
        return v;
    }

    public static short bytes2shortJAVA(byte[] writeBuffer) {
        short v = 0;
        v |= (writeBuffer[0] & 0xFF) << 8;
        v |= (writeBuffer[1] & 0xFF) << 0;
        return v;
    }

    public static short bytes2shortJAVA(byte[] writeBuffer, int offset) {
        short v = 0;
        v |= (writeBuffer[0 + offset] & 0xFF) << 8;
        v |= (writeBuffer[1 + offset] & 0xFF);
        return v;
    }


    public static void int2bytes(int n, byte[] buf) {
        buf[0] = (byte) (n & 0x000000ff);
        buf[1] = (byte) ((n >> 8) & 0x000000ff);
        buf[2] = (byte) ((n >> 16) & 0x000000ff);
        buf[3] = (byte) ((n >> 24) & 0x000000ff);
    }

    public static void writeLong(OutputStream out, long v) throws IOException {
        byte[] writeBuffer = new byte[8];
        writeBuffer[0] = (byte) (v >>> 56);
        writeBuffer[1] = (byte) (v >>> 48);
        writeBuffer[2] = (byte) (v >>> 40);
        writeBuffer[3] = (byte) (v >>> 32);
        writeBuffer[4] = (byte) (v >>> 24);
        writeBuffer[5] = (byte) (v >>> 16);
        writeBuffer[6] = (byte) (v >>> 8);
        writeBuffer[7] = (byte) (v >>> 0);
        out.write(writeBuffer, 0, 8);

    }

    public static void readFully(InputStream in, byte b[], int off, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }

    public static long readLong(InputStream in) throws IOException {
        byte[] readBuffer = new byte[8];
        readFully(in, readBuffer, 0, 8);
        return (((long) readBuffer[0] << 56) +
                ((long) (readBuffer[1] & 255) << 48) +
                ((long) (readBuffer[2] & 255) << 40) +
                ((long) (readBuffer[3] & 255) << 32) +
                ((long) (readBuffer[4] & 255) << 24) +
                ((readBuffer[5] & 255) << 16) +
                ((readBuffer[6] & 255) << 8) +
                ((readBuffer[7] & 255) << 0));
    }


    public static void writeBytes(OutputStream os, byte[] buf, int off, int len) throws IOException {
        byte[] lenbuf = new byte[4];
        lenbuf[0] = (byte) (len & 0x000000ff);
        lenbuf[1] = (byte) ((len >> 8) & 0x000000ff);
        lenbuf[2] = (byte) ((len >> 16) & 0x000000ff);
        lenbuf[3] = (byte) ((len >> 24) & 0x000000ff);
        os.write(lenbuf);
        os.write(buf, off, len);
    }

    public static void writeString(OutputStream os, String s) throws IOException {
        byte[] buf = s.getBytes("UTF-8");
        int len = buf.length;
        byte[] lenbuf = new byte[4];
        lenbuf[0] = (byte) (len & 0x000000ff);
        lenbuf[1] = (byte) ((len >> 8) & 0x000000ff);
        lenbuf[2] = (byte) ((len >> 16) & 0x000000ff);
        lenbuf[3] = (byte) ((len >> 24) & 0x000000ff);
        os.write(lenbuf);
        os.write(buf);
    }

    public static byte[] readBytes(InputStream is) throws IOException {
        byte[] lenbuf = new byte[4];
        //int n = is.read(lenbuf);
        try {
            readFully(is, lenbuf, 0, 4);
        } catch (Exception e) {
            return null;
        }
        int len = bytes2int(lenbuf);
        byte[] sBuf = new byte[len];
        /*
        int l = is.read(sBuf);
        if (l != len) {
            throw new Exception("File Corrupted.");
        } */
        try {
            readFully(is, sBuf, 0, len);
        } catch (Exception e) {
            return null;
        }

        return sBuf;
    }

    public static void writeEntry(OutputStream os, FlexObjectEntry entry) throws Exception {
        if (entry == FlexObjectEntry.empty) {
            CommonTools.writeString(os, "");
            return;
        }
        CommonTools.writeString(os, entry.getName());
        byte flags = 0;
        if (entry.isCompressed()) {
            flags |= compressFlag;
        }
        if (entry.isString()) {
            flags |= stringFlag;
        }
        if (entry.isAdd()) {
            flags |= addFlag;
        }
        CommonTools.writeBytes(os, new byte[]{flags}, 0, 1);
        CommonTools.writeLong(os, entry.getHash());
        CommonTools.writeLong(os,entry.getTxid());
        CommonTools.writeBytes(os, entry.getBytesContent(), 0, entry.getBytesContent().length);

    }


    public static FlexObjectEntry readEntry(InputStream in) throws Exception {

        String name = CommonTools.readString(in);
        if (name == null) {
            return null;
        }
        if (StringUtils.isBlank(name)) {
            return FlexObjectEntry.empty;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        byte[] flagArr = CommonTools.readBytes(in);
        if (flagArr == null) {
            return null;
        }
        byte flag = flagArr[0];
        if ((flag & addFlag) > 0) {
            entry.setAdd(true);
        } else {
            entry.setAdd(false);
        }
        if ((flag & stringFlag) > 0) {
            entry.setString(true);
        } else {
            entry.setString(false);
        }
        if ((flag & compressFlag) > 0) {
            entry.setCompressed(true);
        } else {
            entry.setCompressed(false);
        }
        long hash = CommonTools.readLong(in);
        entry.setHash(hash);
        long txid = CommonTools.readLong(in);
        entry.setTxid(txid);
        try {
            byte[] bytes = CommonTools.readBytes(in);
            entry.setBytesContent(bytes);
        } catch (Exception e) {
            return null;
        }
        return entry;
    }

    public static String readString(InputStream is) throws Exception {
        byte[] lenbuf = new byte[4];
        //int n = is.read(lenbuf);
        try {
            readFully(is, lenbuf, 0, 4);
        } catch (Exception e) {
            return null;
        }
        int len = bytes2int(lenbuf);
        byte[] sBuf = new byte[len];
        /*
        int l = is.read(sBuf);
        if (l != len) {
            throw new Exception("File Corrupted.");
        } */
        readFully(is, sBuf, 0, len);
        return new String(sBuf, "UTF-8");
    }

    public static byte[] zip(byte[] bytes) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream zos = new GZIPOutputStream(bos);
        zos.write(bytes, 0, bytes.length);
        zos.close();
        return bos.toByteArray();
    }

    public static byte[] unzip(byte[] bytes) throws IOException, DataFormatException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        GZIPInputStream zis = new GZIPInputStream(bis);
        byte[] buf = new byte[2048];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int n = zis.read(buf);
        while (n > 0) {
            bos.write(buf, 0, n);
            n = zis.read(buf);
        }
        zis.close();
        return bos.toByteArray();
    }


    public static int compareTwoRange(SortListObject o1min, SortListObject o1max, SortListObject o2min, SortListObject o2max) {
        if (o1max.compareTo(o2min) < 0) {
            return -1;
        }
        if (o1max.compareTo(o2min) == 0) {
            return o1min.compareTo(o2max);
        }
        if (o1max.compareTo(o2min) > 0) {
            return 1;
        }
        return 0;
    }

    public static int compareRangeAndPoint(SortListObject min, SortListObject max, SortListObject p) {
        if (min.compareTo(p) <= 0 && max.compareTo(p) >= 0) {
            return 0;
        }
        if (min.compareTo(p) > 0) {
            return 1;
        }
        if (max.compareTo(p) < 0) {
            return -1;
        }

        //不可能到这里的
        return -10;


    }

    public static SortListObject keyToObj(String key) {
        String[] parts = key.split(SortListObject.KEY_DELIMITER);
        return new SortListObject(parts[0], parts[1]);

    }

    public static int compareKey(String k1, String k2) {
        SortListObject o1 = keyToObj(k1);
        SortListObject o2 = keyToObj(k2);
        return o1.compareTo(o2);
    }


    public static byte[] getAllBytes(InputStream in) throws IOException {
        int n = 0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[2048];
        n = in.read(buf);
        while (n > 0) {
            bos.write(buf, 0, n);
            n = in.read(buf);
        }
        return bos.toByteArray();

    }


    static ObjectMapper objectMapper = new ObjectMapper();
    {
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static byte[] beanToJson(Object bean) throws IOException {
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.writeValueAsBytes(bean);
    }

    public static <T> T bytesToBean(byte[] bytes, Class<T> c) throws IOException {
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return  objectMapper.readValue(bytes,c);
    }

    public static List<String> getIds(List<SortListObject> list) {
        List<String> ids = new Vector();
        try {
            for (int i = 0; i < list.size(); i++) {
                ids.add(list.get(i).getObjid());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ids;
    }

    public static List getObjects(ISortList list, int from ,int num, Class clazz,IFlexObjectFactory flexObjectFactory) throws Exception {
        List<SortListObject> objs = list.getRange(from,num);
        //objs = list.getRange(0,-1);
        List<String> ids = new ArrayList<String>(objs.size());
        for(SortListObject obj : objs){
            ids.add(obj.getObjid());
        }
        List<Object> result = new Vector<Object>();
        List<FlexObjectEntry> entries =  flexObjectFactory.getFlexObjects(ids);
        for(FlexObjectEntry entry : entries){
            if(!FlexObjectEntry.isEmpty(entry)){
                try{
                    Object retObj = bytesToBean(entry.getBytes(),clazz);
                    result.add(retObj);
                }
                catch(Exception e){

                }


            }
        }
        return result;
    }

    public static List getObjects(ISortList list, int from ,int num, Class clazz,IFlexObjectFactory flexObjectFactory,boolean reverse) throws Exception {
        int realFrom = from;
        int realTo = 0;
        if(reverse){
            int size = (int)list.getSize();
            realFrom = size - from-num;
            if(realFrom<0){
                realFrom = 0;
            }
            realTo = size - from;
            num = realTo - realFrom;

        }
        List<SortListObject> objs = list.getRange(realFrom,num);
        //objs = list.getRange(0,-1);
        List<String> ids = new ArrayList<String>(objs.size());
        for(SortListObject obj : objs){
            ids.add(obj.getObjid());
        }
        if(reverse){
            Collections.reverse(ids);
        }
        List<Object> result = new Vector<Object>();
        List<FlexObjectEntry> entries =  flexObjectFactory.getFlexObjects(ids);
        for(FlexObjectEntry entry : entries){
            if(!FlexObjectEntry.isEmpty(entry)){
                Object retObj = bytesToBean(entry.getBytes(),clazz);
                result.add(retObj);
            }
        }
        return result;
    }

    public static List getObjects(List<String> ids,Class clazz,IFlexObjectFactory flexObjectFactory) throws Exception {
        List<Object> result = new Vector<Object>();
        List<FlexObjectEntry> entries =  flexObjectFactory.getFlexObjects(ids);
        for(FlexObjectEntry entry : entries){
            if(!FlexObjectEntry.isEmpty(entry)){
                Object retObj = bytesToBean(entry.getBytes(),clazz);
                result.add(retObj);
            }
        }
        return result;
    }

    static public Object getObject(String objId,Class clazz,IFlexObjectFactory factory) throws Exception {
        FlexObjectEntry entry = factory.getFlexObject(objId);
        if(FlexObjectEntry.isEmpty(entry)){
            return null;
        }
        return bytesToBean(entry.getBytes(),clazz);
    }



    public static String expandEnvVars(String text) {
        Map<String, String> envMap = System.getenv();
        for (Map.Entry<String, String> entry : envMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            text = text.replaceAll("\\$\\{" + key + "\\}", value);
        }
        return text;
    }




}
