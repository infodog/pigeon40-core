package net.xinshi.pigeon.server.distributedserver.flexobjectserver;

import net.xinshi.pigeon.common.Constants;
import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.flexobject.impls.fastsimple.CommonFlexObjectFactory;
import net.xinshi.pigeon.server.distributedserver.BaseServer;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.DefaultHashGenerator;
import net.xinshi.pigeon.util.IdChecker;
import org.apache.distributedlog.LogRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:08
 * To change this template use File | Settings | File Templates.
 */

public class FlexObjectServer extends BaseServer {

    CommonFlexObjectFactory flexObjectFactory;

    Logger logger = Logger.getLogger(FlexObjectServer.class.getName());

    public IFlexObjectFactory getFlexObjectFactory() {
        return flexObjectFactory;
    }

    public void setFlexObjectFactory(CommonFlexObjectFactory flexObjectFactory) {
        this.flexObjectFactory = flexObjectFactory;
    }

    public Map getStatusMap() {
        return flexObjectFactory.getStatusMap();
    }

    private long writeEntryToLog(FlexObjectEntry entry) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        long txid = getNextTxid();
        entry.setTxid(txid);
        CommonTools.writeEntry(os,entry);
        writeLog(txid,os.toByteArray());
        return txid;
    }

    public void doGetContent(InputStream is, OutputStream os) throws Exception {
        String name = CommonTools.readString(is);
        try {
            IdChecker.assertValidId(name);
            String content = flexObjectFactory.getContent(name);
            if (content == null) {
                content = "";
            }
            CommonTools.writeString(os, "OK");
            CommonTools.writeString(os, content);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void doGetContents(InputStream is, OutputStream os) throws Exception {
        List<String> names = new Vector();
        while (true) {
            try {
                String name = CommonTools.readString(is);
                if (name == null) {
                    break;
                }
                IdChecker.assertValidId(name);
                names.add(name);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        List lnames = new ArrayList<String>();
        for (String name : names) {
            lnames.add(name);
        }
        List<String> contents = this.flexObjectFactory.getContents(lnames);
        CommonTools.writeString(os, "OK");
        for (String content : contents) {
            CommonTools.writeString(os, content);
        }
    }

    public void doSaveContent(InputStream is, OutputStream os) throws Exception {
        if(!isMaster()){
            CommonTools.writeString(os,"error: not master is readonly!");
            return;
        }
        String name = CommonTools.readString(is);
        IdChecker.assertValidId(name);


        String content = CommonTools.readString(is);
        try {
            if (name.getBytes().length > 120) {
                throw new Exception("name too big, name = " + name);
            }
            if (content == null) {
                content = "";
            }
            boolean isCompressed = false;
            byte[] bytes = content.getBytes("UTF-8");
            if (bytes.length > 512) {
                bytes = CommonTools.zip(bytes);
                isCompressed = true;
            }
            if (bytes.length > 1024*1024) {
                throw new Exception("content too big for flexobjectclient");
            }

            FlexObjectEntry entry = new FlexObjectEntry();
            entry.setAdd(false);
            entry.setBytesContent(bytes);
            entry.setName(name);
            entry.setHash(DefaultHashGenerator.hash(name));
            entry.setString(true);
            entry.setCompressed(isCompressed);
            try {
                long txid = writeEntryToLog(entry);
                entry.setTxid(txid);
            }
            catch(Exception e){
                switchToSlave();
                CommonTools.writeString(os, e.getMessage());
                return;
            }

            flexObjectFactory.saveFlexObject(entry);

            CommonTools.writeString(os, "ok");
        } catch (Exception e) {
            CommonTools.writeString(os, e.getMessage());
        }
    }

    public ByteArrayOutputStream doCommand(InputStream in) throws Exception {
        try {
            String cmd = CommonTools.readString(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (cmd.equals("version")) {
                long ver = flexObjectFactory.verLogger.getVersion();
                CommonTools.writeString(out, String.valueOf(ver));
                return out;
            }
            return null;
        } catch (Exception e) {
            throw e;
        }
    }



    public void doSaveFlexObject(InputStream is, OutputStream os) throws Exception {
        if(!isMaster()){
            CommonTools.writeString(os,"error: not master is readonly!");
            return;
        }
        try {
            FlexObjectEntry entry = CommonTools.readEntry(is);
            if (entry == null) {
                CommonTools.writeString(os, "incorrect request to server.");
                logger.log(Level.SEVERE, "incorrect request to server.");
            } else {
                String name = entry.getName();
                IdChecker.assertValidId(name);
                if (name == null || name.length() > Constants.DB_KEY_MAX_LENGTH) {
                    throw new Exception("name = " + name + " is null or length > " + Constants.DB_KEY_MAX_LENGTH);
                }
                writes++;

                try {
                    long txid = writeEntryToLog(entry);
                    entry.setTxid(txid);
                }
                catch(Exception e){
                    switchToSlave();
                    CommonTools.writeString(os, e.getMessage());
                    return;
                }
                flexObjectFactory.saveFlexObject(entry);
                CommonTools.writeString(os, "ok");
            }
        } catch (Exception e) {
            CommonTools.writeString(os, e.getMessage());
//            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }


    public void doSaveFlexObjects(InputStream is, OutputStream os) throws IOException {
        if(!isMaster()){
            CommonTools.writeString(os,"error: not master is readonly!");
            return;
        }
        List<FlexObjectEntry> objs = new Vector<FlexObjectEntry>();
        try {
            FlexObjectEntry entry = CommonTools.readEntry(is);
            while (entry != null) {
                String name = entry.getName();
                IdChecker.assertValidId(name);
                if (name == null || name.length() > Constants.DB_KEY_MAX_LENGTH) {
                    throw new Exception("name = " + name + " is null or length > " + Constants.DB_KEY_MAX_LENGTH);
                }
                objs.add(entry);
                entry = CommonTools.readEntry(is);
            }
            for(FlexObjectEntry obj : objs){
                try {
                    long txid = writeEntryToLog(obj);
                    obj.setTxid(txid);
                }
                catch(Exception e){
                    switchToSlave();
                    CommonTools.writeString(os, e.getMessage());
                    return;
                }
                flexObjectFactory.saveFlexObject(entry);
            }
            flexObjectFactory.saveFlexObjects(objs);
            if (flexObjectFactory.getDirtyCacheSize() > 60000) {
                CommonTools.writeString(os, "TooFast");
                CommonTools.writeLong(os, flexObjectFactory.getDirtyCacheSize());
            } else {
                CommonTools.writeString(os, "ok");
            }
        } catch (Exception e) {
            CommonTools.writeString(os, e.getMessage());
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public void doGetFlexObject(InputStream is, OutputStream os) throws Exception {
        try {
            String name = CommonTools.readString(is);
            IdChecker.assertValidId(name);
            FlexObjectEntry entry = flexObjectFactory.getFlexObject(name);
            if (entry == null) {
                CommonTools.writeString(os, "isnull");
            } else {
                CommonTools.writeString(os, "ok");
                CommonTools.writeEntry(os, entry);
            }
        } catch (Exception e) {
            CommonTools.writeString(os, e.getMessage());
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public void doGetFlexObjects(InputStream is, OutputStream os) throws Exception {
        try {
            List<String> names = new Vector();
            while (true) {
                try {
                    String name = CommonTools.readString(is);
                    IdChecker.assertValidId(name);
                    if (name == null) {
                        break;
                    }
                    names.add(name);
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            List<FlexObjectEntry> contents = this.flexObjectFactory.getFlexObjects(names);
            CommonTools.writeString(os, "ok");
            for (FlexObjectEntry entry : contents) {
                if (entry == null) {
                    CommonTools.writeEntry(os, FlexObjectEntry.empty);
                } else {
                    CommonTools.writeEntry(os, entry);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            super.stop();
            flexObjectFactory.stop();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "can not stop flexobjectfactory", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void updateLog(LogRecord logRec) {
        InputStream is = logRec.getPayLoadInputStream();
        try {
            FlexObjectEntry entry = CommonTools.readEntry(is);
            flexObjectFactory.saveFlexObject(entry);
        } catch (Exception e) {
            //都不能够从日志恢复，退出
            e.printStackTrace();
            System.exit(-1);
        }


    }

    @Override
    protected long getLocalLastTxId() {
        return flexObjectFactory.verLogger.getVersion();
    }

    public void set_state_word(int state_word) throws Exception {
        try {
            flexObjectFactory.set_state_word(state_word);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "can not set_state_word flexobjectfactory", e);
        }
    }
}

