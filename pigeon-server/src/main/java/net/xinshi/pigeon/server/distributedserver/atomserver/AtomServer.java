package net.xinshi.pigeon.server.distributedserver.atomserver;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.atom.IServerAtom;
import net.xinshi.pigeon.atom.impls.dbatom.FastAtom;
import net.xinshi.pigeon.server.distributedserver.BaseServer;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.server.distributedserver.writeaheadlog.LogRecord;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.IdChecker;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午3:55
 * To change this template use File | Settings | File Templates.
 */

public class AtomServer extends BaseServer {

    IServerAtom atom;
    Logger logger = Logger.getLogger(AtomServer.class.getName());

    long writeCreateAndSetLog(String name,int value) throws IOException, ExecutionException, InterruptedException {

//        long txid = getNextTxid();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        CommonTools.writeString(os,"createAndSet");
        CommonTools.writeString(os,name);
        CommonTools.writeLong(os,value);
        byte[] data = os.toByteArray();

        LogRecord r = new LogRecord();
        r.setValue(data);
        r.setKey(null);
        txid = writeLog(r);
        return txid;
    }

    long writeGreaterAndIncLog(String name,int value,int inc) throws IOException, ExecutionException, InterruptedException {
//        long txid = getNextTxid();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        CommonTools.writeString(os,"greaterAndInc");
        CommonTools.writeString(os,name);
        CommonTools.writeLong(os,value);
        CommonTools.writeLong(os,inc);
        byte[] data = os.toByteArray();
        LogRecord r = new LogRecord();
        r.setValue(data);
        r.setKey(null);
        txid = writeLog(r);
        return txid;
    }

    long writeLessAndIncLog(String name,int value,int inc) throws IOException, ExecutionException, InterruptedException {
        long txid = getNextTxid();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        CommonTools.writeString(os,"lessAndInc");
        CommonTools.writeString(os,name);
        CommonTools.writeLong(os,value);
        CommonTools.writeLong(os,inc);
        byte[] data = os.toByteArray();
        LogRecord r = new LogRecord();
        r.setValue(data);
        r.setKey(null);
        txid = writeLog(r);
        return txid;
    }


    public ByteArrayOutputStream doCommand(InputStream in) throws Exception {
        try {
            String cmd = CommonTools.readString(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (cmd.equals("version")) {
                long ver = ((FastAtom) getAtom()).verLogger.getVersion();
                CommonTools.writeString(out, String.valueOf(ver));
                return out;
            }
            return null;
        } catch (Exception e) {
            throw e;
        }
    }

    public void doGetValue(InputStream is, ByteArrayOutputStream os) throws Exception {
        String name = CommonTools.readString(is);
        try {
            Long r = atom.get(name);
            String result = "";
            if (r == null) {
                result = "";
            } else {
                result = String.valueOf(r);
            }
            CommonTools.writeString(os, "ok");
            CommonTools.writeString(os, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void doCreateAndSet(InputStream is, ByteArrayOutputStream os) throws Exception {
        if(!isMaster()){
            CommonTools.writeString(os,"error: not master is readonly!");
            return;
        }
        String name = CommonTools.readString(is);
        String initValue = CommonTools.readString(is);
        int value = 0;
        try {
            IdChecker.assertValidId(name);
            value = Integer.parseInt(initValue);

            long txid = 0;
            try {
                txid = writeCreateAndSetLog(name, value);
            } catch (Exception e) {
                e.printStackTrace();
                switchToSlave();
                CommonTools.writeString(os, "error:" + e.getMessage());
                return;
            }
            atom.createAndSet(name, value,txid);
            CommonTools.writeString(os, "ok");
        } catch (Exception e) {
            e.printStackTrace();
            CommonTools.writeString(os, e.getMessage());
        }
    }

    public void doGreaterAndInc(InputStream is, ByteArrayOutputStream os) throws Exception {
        if(!isMaster()){
            CommonTools.writeString(os,"error: not master is readonly!");
            return;
        }
        String name = CommonTools.readString(is);
        if(StringUtils.isBlank(name)){
            logger.log(Level.SEVERE,"doGreaterAndInc,name is null");
            CommonTools.writeString(os, "failed");
            return;

        }
        String testValue = CommonTools.readString(is);
        String incValue = CommonTools.readString(is);
        int value = 0;
        int inc = 0;
        try {
            value = Integer.parseInt(testValue);
            inc = Integer.parseInt(incValue);
            long txid = 0;
            try {
                txid = writeGreaterAndIncLog(name, value,inc);
            } catch (Exception e) {
                e.printStackTrace();
                switchToSlave();
                CommonTools.writeString(os, "failed:" + e.getMessage());
                return;
            }
            long r = atom.greaterAndIncReturnLong(name, value, inc,txid);
            CommonTools.writeString(os, "ok");
            CommonTools.writeString(os, "" + r);
        } catch (Throwable e) {
            if(e.getMessage()!=null) {
                if (e.getMessage().compareTo("return false") == 0) {
                    CommonTools.writeString(os, "failed");
                    return;
                }
            }
            logger.info("name is null:" + (null==name));
            logger.info("value is null:" + (null==incValue));
            e.printStackTrace();
            if(e.getMessage()!=null) {
                CommonTools.writeString(os, e.getMessage());
            }
            else{
                CommonTools.writeString(os,"failed");
                System.out.println("atom name=" + null);
            }
        }
    }

    public void doLessAndInc(InputStream is, ByteArrayOutputStream os) throws Exception {
        if(!isMaster()){
            CommonTools.writeString(os,"error: not master is readonly!");
            return;
        }
        String name = CommonTools.readString(is);
        String testValue = CommonTools.readString(is);
        String incValue = CommonTools.readString(is);
        int value = 0;
        int inc = 0;
        try {
            value = Integer.parseInt(testValue);
            inc = Integer.parseInt(incValue);
            long txid = 0;
            try {
                txid = writeLessAndIncLog(name, value,inc);
            } catch (Exception e) {
                e.printStackTrace();
                switchToSlave();
                CommonTools.writeString(os, "failed:" + e.getMessage());
                return;
            }
            long r = atom.lessAndIncReturnLong(name, value, inc,txid);
            CommonTools.writeString(os, "ok");
            CommonTools.writeString(os, "" + r);
        } catch (Throwable e) {
            if(e.getMessage()!=null) {
                if (e.getMessage().compareTo("return false") == 0) {
                    CommonTools.writeString(os, "failed");
                    return;
                }
            }
            logger.info("name is null:" + (null==name));
            logger.info("value is null:" + (null==incValue));
            e.printStackTrace();
            if(e.getMessage()!=null) {
                CommonTools.writeString(os, e.getMessage());
            }
            else{
                CommonTools.writeString(os,"failed");
                System.out.println("atom name=" + null);
            }
        }
    }

    public void doGetAtoms(InputStream is, ByteArrayOutputStream os) throws Exception {
        List<String> atomIds = new Vector<String>();
        String atomId = CommonTools.readString(is);
        while (atomId != null) {
            atomIds.add(atomId);
            atomId = CommonTools.readString(is);
        }
        List<Long> atoms = atom.getAtoms(atomIds);
        CommonTools.writeString(os, "ok");
        for (Long v : atoms) {
            if (v != null) {
                CommonTools.writeString(os, "" + v);
            } else {
                CommonTools.writeString(os, "null");
            }
        }
    }

    public IServerAtom getAtom() {
        return atom;
    }

    public void setAtom(IServerAtom atom) {
        this.atom = atom;
    }

    public Map getStatusMap() {
        return ((FastAtom) atom).getStatusMap();
    }

    @Override
    public void stop() {
        try {
            ((FastAtom) atom).stop();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "can not stop atomServer.", e);
        }
    }

    @Override
    protected void updateLog(LogRecord logRec) {
        long txid = logRec.getOffset();
        InputStream is = new ByteArrayInputStream(logRec.getValue());
        try {
            String action = CommonTools.readString(is);
            if(action.equals("createAndSet")){
                String name = CommonTools.readString(is);
                long value = CommonTools.readLong(is);
                atom.createAndSet(name, (int) value,txid);
            }
            if(action.equals("greaterAndInc")){
                String name = CommonTools.readString(is);
                long value = CommonTools.readLong(is);
                long inc = CommonTools.readLong(is);
                atom.greaterAndInc(name, (int) value, (int) inc,txid);
            }
            if(action.equals("lessAndInc")){
                String name = CommonTools.readString(is);
                long value = CommonTools.readLong(is);
                long inc = CommonTools.readLong(is);
                atom.lessAndInc(name, (int) value, (int) inc,txid);
            }
        }
        catch(Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    protected long getLocalLastTxId() {
        try {
            return atom.getLastTxid();
        } catch (Exception e) {
            e.printStackTrace();
            //都获取不了本地日志，直接退出
            System.exit(-1);
            return 0;
        }
    }

}

