package net.xinshi.pigeon50.client.distributedclient.flexobjectclient;

import net.xinshi.pigeon50.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon.util.IdChecker;
import net.xinshi.pigeon.common.Constants;
import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.LRUMap;
import net.xinshi.pigeon.util.ThreadLocalStorage;
import org.apache.commons.lang.StringUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-7
 * Time: 上午11:01
 * To change this template use File | Settings | File Templates.
 */

public class DistributedFlexObjectFactory implements IFlexObjectFactory {

    String type = "flexobject";
    int sizeToCompress = 512;
    private NodesDispatcher nodesDispatcher = null;
    Map mapConstant;
    ThreadLocalStorage tlsCache = new ThreadLocalStorage();

    Logger logger = Logger.getLogger(DistributedFlexObjectFactory.class.getName());

    public int getSizeToCompress() {
        return sizeToCompress;
    }

    public void setSizeToCompress(int sizeToCompress) {
        this.sizeToCompress = sizeToCompress;
    }

    public DistributedFlexObjectFactory(NodesDispatcher nodesDispatcher) {
        this.nodesDispatcher = nodesDispatcher;
    }

    @Override
    public String getContent(String name) throws Exception {
        IdChecker.assertValidId(name);
        boolean fresh = false;
        FlexObjectEntry entry = getTlsCache(name);
        if (entry == null) {
            fresh = true;
            entry = getFlexObject(name);
        }
        if (entry == null) {
            return null;
        }
        if (fresh) {
            putTlsCache(name, entry);
        }
        return entry.getContent();
    }

    public void clearCache(String name) throws Exception {
        IdChecker.assertValidId(name);
        removeTlsCache(name);
    }

    public String getConstant(String name) throws Exception {
        IdChecker.assertValidId(name);
        FlexObjectEntry entry = (FlexObjectEntry) mapConstant.get(name);
        if (entry == FlexObjectEntry.empty) {
            return null;
        }
        if (entry != null) {
            return entry.getContent();
        }
        entry = getFlexObject(name);
        if (entry != null) {
            mapConstant.put(name, entry);
            return entry.getContent();
        } else {
            mapConstant.put(name, FlexObjectEntry.empty);
            return null;
        }
    }

    public void saveContent(String name, String content) throws Exception {
        IdChecker.assertValidId(name);
        if (content == null) {
            content = "";
        }
        byte[] bytes = content.getBytes("utf-8");
        boolean isCompressed = false;
        if (bytes.length > this.sizeToCompress) {
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        entry.setAdd(false);
        entry.setBytesContent(bytes);
        entry.setCompressed(isCompressed);
        entry.setString(true);
        entry.setHash(0);
        saveFlexObject(entry);
        putTlsCache(name, entry);
    }

    private List<String> getOrigContents(List<String> names) throws Exception {
        List<FlexObjectEntry> entries = getFlexObjects(names);
        List<String> result = new Vector<String>();
        for (FlexObjectEntry entry : entries) {
            result.add(entry.getContent());
        }
        return result;
    }

    public List<String> getContents(List<String> names) throws Exception {
        for(String name : names){
            IdChecker.assertValidId(name);
        }
        if (!tlsCache.isOpen()) {
            return getOrigContents(names);
        }
        Map<String, FlexObjectEntry> tmpMap = new HashMap<String, FlexObjectEntry>();
        List<String> tmpList = new ArrayList<String>();
        for (String name : names) {
            FlexObjectEntry entry = getTlsCache(name);
            if (entry != null) {
                tmpMap.put(name, entry);
            } else {
                tmpList.add(name);
            }
        }
        List<FlexObjectEntry> entries = getFlexObjects(tmpList);
        List<String> result = new Vector<String>();
        for (String name : names) {
            FlexObjectEntry entry = tmpMap.get(name);
            if (entry == null) {
                entry = entries.get(0);
                if (entry != FlexObjectEntry.empty && !entry.getName().equals(name)) {
                    throw new Exception("pigeon tls = true, getContents error ... ");
                }
                putTlsCache(name, entry);
                entries.remove(0);
            }
            result.add(entry.getContent());
        }
        return result;
    }

    @Override
    public void addContent(String name, String value) throws Exception {
        IdChecker.assertValidId(name);
        byte[] bytes = value.getBytes("utf-8");
        boolean isCompressed = false;
        if (bytes.length > this.sizeToCompress) {
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        entry.setAdd(true);
        entry.setBytesContent(bytes);
        entry.setCompressed(isCompressed);
        entry.setString(true);
        entry.setHash(0);
        saveFlexObject(entry);
        putTlsCache(name, entry);
    }

    @Override
    public void addContent(String name, byte[] bytes) throws Exception {
        IdChecker.assertValidId(name);
        boolean isCompressed = false;
        if (bytes.length > this.sizeToCompress) {
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        entry.setAdd(true);
        entry.setBytesContent(bytes);
        entry.setCompressed(isCompressed);
        entry.setString(false);
        entry.setHash(0);
        saveFlexObject(entry);
        putTlsCache(name, entry);
    }

    void ServerIsBusy(String s) throws Exception {
        if (s.indexOf("DuplicateService.syncQueueOverflow()") >= 0) {
            System.out.println("flexobject pigeon server very busy! sleep 100 ms ......");
            Thread.sleep(100);
        }
    }

    @Override
    public void saveFlexObject(FlexObjectEntry entry) throws Exception {
        IdChecker.assertValidId(entry.getName());
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "saveFlexObject");
            CommonTools.writeEntry(out, entry);
            InputStream in = nodesDispatcher.commit(type, entry.getName(), out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {
                return;
            } else {
                ServerIsBusy(state);
                logger.log(Level.SEVERE, state);
                throw new Exception(state);
            }
        } finally {
        }
    }

    @Override
    public void saveBytes(String name, byte[] content) throws Exception {
        IdChecker.assertValidId(name);
        boolean isCompressed = false;
        if (content.length > sizeToCompress) {
            content = CommonTools.zip(content);
            isCompressed = true;
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setName(name);
        entry.setAdd(false);
        entry.setBytesContent(content);
        entry.setCompressed(isCompressed);
        entry.setString(false);
        entry.setHash(0);
        saveFlexObject(entry);
        putTlsCache(name, entry);
    }

    @Override
    public int deleteContent(String name) throws Exception {
        IdChecker.assertValidId(name);
        saveContent(name, "");
        return 0;
    }

    private PigeonFuture getFlexObjectAsync(String name) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getFlexObject");
        CommonTools.writeString(out, name);
        PigeonFuture pf = nodesDispatcher.commitAsync(type, name, out);
        return pf;
    }

    private PigeonFuture putFlexObjectAsync(FlexObjectEntry entry) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "saveFlexObject");
        CommonTools.writeEntry(out, entry);
        PigeonFuture pf = nodesDispatcher.commitAsync(type, entry.getName(), out);
        return pf;
    }

    @Override
    public List<FlexObjectEntry> getFlexObjects(List<String> names) throws Exception {
        try {
            List<PigeonFuture> listPF = new ArrayList<PigeonFuture>();
            for (String name : names) {
                if (StringUtils.isNotBlank(name)) {
                    PigeonFuture pf = getFlexObjectAsync(name);
                    listPF.add(pf);
                } else {
                    listPF.add(null);
                }
            }
            List<FlexObjectEntry> listFOE = new Vector<FlexObjectEntry>();
            for (PigeonFuture pf : listPF) {
                if (pf != null) {
                    boolean ok = pf.waitme(1000 * 60);
                    if (ok) {
                        InputStream in = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
                        String state = CommonTools.readString(in);
                        if (StringUtils.equals("ok", state)) {
                            FlexObjectEntry content = CommonTools.readEntry(in);
                            listFOE.add(content);
                            continue;
                        }
                    }
                }
                listFOE.add(FlexObjectEntry.empty);
            }
            return listFOE;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public byte[] getBytes(String name) throws Exception {
        FlexObjectEntry foe = null;
        try {
            foe = getFlexObject(name);
            if (foe == null) return null;
            return foe.getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public FlexObjectEntry getFlexObject(String name) throws SQLException, Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CommonTools.writeString(out, "getFlexObject");
            CommonTools.writeString(out, name);
            InputStream in = nodesDispatcher.commit(type, name, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {
                FlexObjectEntry content = CommonTools.readEntry(in);
                return content;
            } else {
                return null;
            }
        } finally {
        }
    }

    @Override
    public void saveFlexObjects(List<FlexObjectEntry> objs) throws Exception {
        try {
            List<PigeonFuture> listPF = new ArrayList<PigeonFuture>();
            for (FlexObjectEntry obj : objs) {
                PigeonFuture pf = putFlexObjectAsync(obj);
                listPF.add(pf);
            }
            int i = 0;
            for (PigeonFuture pf : listPF) {
                ++i;
                if (pf == null) {
                    throw new Exception("saveFlexObjects PigeonFuture == null no = " + i);
                }
                boolean ok = pf.waitme(1000 * 60);
                if (ok) {
                    InputStream in = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
                    String state = CommonTools.readString(in);
                    if (StringUtils.equals("ok", state)) {
                        continue;
                    }
                }
                throw new Exception("saveFlexObjects waitme != ok no = " + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init() throws Exception {
        System.out.println("distributed flexobject init ...... ");
        LRUMap cache = new LRUMap<String, FlexObjectEntry>(10000);
        mapConstant = Collections.synchronizedMap(cache);
    }

    @Override
    public void stop() throws Exception {
        System.out.println("distributed flexobject stop ...... ");
    }

    @Override
    public void set_state_word(int state_word) throws Exception {
        System.out.println("distributed flexobject set_state_word ...... ");
    }

    @Override
    public void setTlsMode(boolean open) {
        tlsCache.setOpen(open);
    }

    private FlexObjectEntry getTlsCache(String name) {
        if (tlsCache.isOpen()) {
            return (FlexObjectEntry) tlsCache.getMap(name);
        }
        return null;
    }

    private void putTlsCache(String name, FlexObjectEntry entry) {
        if (tlsCache.isOpen()) {
            tlsCache.putMap(name, entry);
        }
    }

    private void removeTlsCache(String name){
        if (tlsCache.isOpen()) {
            tlsCache.putMap(name,null);
        }
    }

    @Override
    public void saveTemporaryContent(String name, String content) throws Exception {
        if (tlsCache.isOpen()) {
            if (content == null) {
                content = "";
            }
            byte[] bytes = content.getBytes("utf-8");
            boolean isCompressed = false;
            if (bytes.length > this.sizeToCompress) {
                bytes = CommonTools.zip(bytes);
                isCompressed = true;
            }
            FlexObjectEntry entry = new FlexObjectEntry();
            entry.setName(name);
            entry.setAdd(false);
            entry.setBytesContent(bytes);
            entry.setCompressed(isCompressed);
            entry.setString(true);
            entry.setHash(0);
            putTlsCache(name, entry);
        } else {
            throw new Exception("saveTemporaryContent() must setTlsMode(true) ... ");
        }
    }

    @Override
    public long getLastTxid() throws Exception {
        return 0;
    }

}

