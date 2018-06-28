package net.xinshi.pigeon.server.distributedserver.idserver;

import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.idgenerator.IIDGeneratorServer;
import net.xinshi.pigeon.idgenerator.impl.MysqlIDGenerator;
import net.xinshi.pigeon.server.distributedserver.BaseServer;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.distributedlog.LogRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:23
 * To change this template use File | Settings | File Templates.
 */

public class IdServer extends BaseServer {

    long writeGetIdAndForwardLog(String idName, int count) throws IOException {
        long txid = getNextTxid();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        CommonTools.writeString(os,idName);
        CommonTools.writeLong(os,count);
        byte[] data = os.toByteArray();
        writeLog(txid,data);
        return txid;
    }

    MysqlIDGenerator idgenerator;
    final long MAX_ID_COUNT_EACH_TIME = 20000;

    public IIDGeneratorServer getIdgenerator() {
        return idgenerator;
    }

    public void setIdgenerator(MysqlIDGenerator idgenerator) {
        this.idgenerator = idgenerator;
    }

    public Map getStatusMap() {
        return idgenerator.getStatusMap();
    }

    public void doGetNextIds(InputStream in, ByteArrayOutputStream out) throws Exception {
        if(!isMaster()){
            CommonTools.writeString(out,"error: not master is readonly!");
            return;
        }
        String idName = CommonTools.readString(in);
        Tools.checkNameLength(idName);
        long count = CommonTools.readLong(in);
        if (count > MAX_ID_COUNT_EACH_TIME) {
            CommonTools.writeString(out, "exceed MAX_ID_COUNT_EACH_TIME:" + count);
            return;
        }
        long from = 0;
        long to = 0;
        long txid = 0;
        try {
            txid = writeGetIdAndForwardLog(idName, (int) count);
        } catch (Exception e) {
            e.printStackTrace();
            switchToSlave();
            CommonTools.writeString(out, "failed:" + e.getMessage());
            return;
        }
        from = idgenerator.getIdAndForward(idName, (int) count,txid);
        to = from + count - 1;
        CommonTools.writeString(out, "ok");
        CommonTools.writeLong(out, from);
        CommonTools.writeLong(out, to);
    }

    public void setSkipValue(InputStream in, ByteArrayOutputStream out) throws Exception {
        doGetNextIds(in,out);
    }



    public ByteArrayOutputStream doCommand(InputStream in) throws Exception {
        try {
            String cmd = CommonTools.readString(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (cmd.equals("version")) {
                long ver = (((MysqlIDGenerator) getIdgenerator())).verLogger.getVersion();
                CommonTools.writeString(out, String.valueOf(ver));
                return out;
            }
            return null;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void stop() {
    }

    @Override
    protected void updateLog(LogRecord logRec) {
        try {
            long txid = logRec.getTransactionId();
            InputStream is = logRec.getPayLoadInputStream();
            String name = CommonTools.readString(is);
            long count = CommonTools.readLong(is);
            idgenerator.getIdAndForward(name, (int) count, txid);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
            return;
        }
        return;
    }

    @Override
    protected long getLocalLastTxId() {
        return idgenerator.getVersion();
    }
}


