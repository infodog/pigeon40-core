package net.xinshi.pigeon50.client.distributedclient.atomclient;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon50.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon.util.IdChecker;

import net.xinshi.pigeon.common.Constants;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-7
 * Time: 下午12:13
 * To change this template use File | Settings | File Templates.
 */

public class DistributedAtom implements IIntegerAtom {

    String type = "atom";
    private NodesDispatcher nodesDispatcher = null;

    Logger logger = Logger.getLogger(DistributedAtom.class.getName());

    public DistributedAtom(NodesDispatcher nodesDispatcher) {
        this.nodesDispatcher = nodesDispatcher;
    }

    void ServerIsBusy(String s) throws Exception {
        if (s.indexOf("DuplicateService.syncQueueOverflow()") >= 0) {
            System.out.println("atom pigeon server very busy! sleep 100 ms ......");
            Thread.sleep(100);
        }
    }

    public boolean createAndSet(String name, Integer initValue) throws Exception {
        IdChecker.assertValidId(name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "createAndSet");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + initValue);
        try {
            InputStream in = nodesDispatcher.commit(type, name, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String s = CommonTools.readString(in);
            s = StringUtils.trim(s);
            if (s.equals("ok")) {
                return true;
            } else {
                ServerIsBusy(s);
                throw new Exception("save failed." + s);
            }
        } finally {
        }
    }

    public PigeonFuture getAsync(String name) throws Exception {
        IdChecker.assertValidId(name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getValue");
        CommonTools.writeString(out, name);
        try {
            PigeonFuture pf = nodesDispatcher.commitAsync(type, name, out);
            return pf;
        } finally {
        }
    }

    public Long get(String name) throws Exception {
        IdChecker.assertValidId(name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "getValue");
        CommonTools.writeString(out, name);
        try {
            InputStream in = nodesDispatcher.commit(type, name, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String s = CommonTools.readString(in);
                s = StringUtils.trim(s);
                if (s.length() == 0) {
                    return null;
                }
                long r = Long.parseLong(s);
                return r;
            } else {
                return null;
            }
        } finally {
        }
    }

    @Override
    public List<Long> getAtoms(List<String> atomIds) throws Exception {
        for(String atomId : atomIds){
            IdChecker.assertValidId(atomId);
        }
        List<PigeonFuture> listPF = new ArrayList<PigeonFuture>();
        for (String id : atomIds) {
            PigeonFuture pf = getAsync(id);
            listPF.add(pf);
        }
        List<Long> listLong = new ArrayList<Long>();
        for (PigeonFuture pf : listPF) {
            if (pf != null) {
                boolean ok = pf.waitme(1000 * 60);
                if (ok) {
                    InputStream in = new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
                    String state = CommonTools.readString(in);
                    if (StringUtils.equals(state, "ok")) {
                        String s = CommonTools.readString(in);
                        s = StringUtils.trim(s);
                        Long r = null;
                        if (s.length() > 0) {
                            r = Long.parseLong(s);
                        }
                        listLong.add(r);
                        continue;
                    }
                }
            }
            listLong.add(null);
        }
        return listLong;
    }

    public long greaterAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        IdChecker.assertValidId(name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "greaterAndInc");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + testValue);
        CommonTools.writeString(out, "" + incValue);
        try {
            InputStream in = nodesDispatcher.commit(type, name, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String s = CommonTools.readString(in);
                s = StringUtils.lowerCase(s);
                try {
                    return Long.parseLong(s);
                } catch (Exception e) {
                    throw new Exception("save failed. " + s);
                }
            } else {
                ServerIsBusy(state);
                throw new Exception("server error." + state);
            }

        } finally {
        }
    }

    @Override
    public boolean greaterAndInc(String name, int testValue, int incValue) throws Exception {
        IdChecker.assertValidId(name);
        try {
            long rl = greaterAndIncReturnLong(name, testValue, incValue);
            return true;
        } catch (Exception e) {
            if (e.getMessage().compareTo("server error.failed") == 0) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public boolean lessAndInc(String name, int testValue, int incValue) throws Exception {
        IdChecker.assertValidId(name);
        try {
            long rl = lessAndIncReturnLong(name, testValue, incValue);
            return true;
        } catch (Exception e) {
            if (e.getMessage().compareTo("server error.failed") == 0) {
                return false;
            }
            throw e;
        }
    }

    public long lessAndIncReturnLong(String name, int testValue, int incValue) throws Exception {
        IdChecker.assertValidId(name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, "lessAndInc");
        CommonTools.writeString(out, name);
        CommonTools.writeString(out, "" + testValue);
        CommonTools.writeString(out, "" + incValue);
        try {
            InputStream in = nodesDispatcher.commit(type, name, out);
            if (in == null) {
                throw new Exception("server error; timeout");
            }
            String state = CommonTools.readString(in);
            if (StringUtils.equals(state, "ok")) {
                String s = CommonTools.readString(in);
                s = StringUtils.trim(s);
                try {
                    return Long.parseLong(s);
                } catch (Exception e) {
                    throw new Exception("save failed. " + s);
                }
            } else {
                ServerIsBusy(state);
                throw new Exception("server error." + state);
            }
        } finally {
        }
    }

    public void init() throws Exception {
        System.out.println("DistributedAtom do init ...... ");
    }

    public void set_state_word(int state_word) throws Exception {
        System.out.println("DistributedAtom do set_state_word ...... ");
    }

}

