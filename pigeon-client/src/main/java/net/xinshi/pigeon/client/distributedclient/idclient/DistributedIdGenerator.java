package net.xinshi.pigeon.client.distributedclient.idclient;

import net.xinshi.pigeon.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon.util.IdChecker;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-7
 * Time: 下午2:23
 * To change this template use File | Settings | File Templates.
 */

public class DistributedIdGenerator implements IIDGenerator {

    String type = "idserver";
    long idNumPerRound = 10000L;
    private NodesDispatcher nodesDispatcher = null;

    Logger logger = Logger.getLogger(DistributedIdGenerator.class.getName());

    public DistributedIdGenerator(NodesDispatcher nodesDispatcher, long idNumPerRound) {
        this.nodesDispatcher = nodesDispatcher;
        this.idNumPerRound = idNumPerRound;
    }

    public long getIdNumPerRound() {
        return idNumPerRound;
    }

    public void setIdNumPerRound(long idNumPerRound) {
        this.idNumPerRound = idNumPerRound;
    }

    class IdPair {
        public IdPair() {
            curVal = 0;
            maxVal = 0;
        }
        public int curVal;
        public int maxVal;
    }

    ConcurrentHashMap Ids = null;

    void ServerIsBusy(String s) throws Exception {
        if (s.indexOf("DuplicateService.syncQueueOverflow()") >= 0) {
            System.out.println("idserver pigeon server very busy! sleep 100 ms ......");
            Thread.sleep(100);
        }
    }

    @Override
    public synchronized long getId(String name) throws Exception {
        IdChecker.assertValidId(name);
        int idForThisTime;
        if (Ids == null) {
            Ids = new ConcurrentHashMap();
        }
        IdPair id = (IdPair) Ids.get(name);
        if (id != null && id.curVal <= id.maxVal) {
            idForThisTime = id.curVal++;
            return idForThisTime;
        }
        id = new IdPair();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getIdRange");
        CommonTools.writeString(out, name);
        CommonTools.writeLong(out, idNumPerRound);
        try {
            InputStream in = nodesDispatcher.commit(type, name, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {
                long from = CommonTools.readLong(in);
                long to = CommonTools.readLong(in);
                id.curVal = (int) from;
                id.maxVal = (int) to;
                long c = id.curVal++;
                Ids.put(name, id);
                return c;
            } else {
                ServerIsBusy(state);
                throw new Exception("server error, state=" + state);
            }
        } finally {
        }
    }

    public long setSkipValue(String name, long value) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "setSkipValue");
        CommonTools.writeString(out, name);
        CommonTools.writeLong(out, value);
        try {
            InputStream in = nodesDispatcher.commit(type, name, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals("ok", state)) {
                long from = CommonTools.readLong(in);
                long to = CommonTools.readLong(in);
                return to;
            } else {
                ServerIsBusy(state);
                throw new Exception("server error, state=" + state);
            }
        } finally {
        }
    }

    public void init() throws Exception {
        Ids = new ConcurrentHashMap();
        System.out.println("DistributedIdGenerator DO INIT ...... ");
    }

    public void set_state_word(int state_word) throws Exception {
        System.out.println("DistributedIdGenerator DO set_state_word ...... ");
    }

}

