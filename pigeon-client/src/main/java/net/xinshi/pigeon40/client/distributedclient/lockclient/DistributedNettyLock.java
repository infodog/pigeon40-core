package net.xinshi.pigeon40.client.distributedclient.lockclient;

import net.xinshi.pigeon40.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon40.client.net.xinshi.pigeon.client.zookeeper.PigeonNode;
import net.xinshi.pigeon40.client.net.xinshi.pigeon.client.zookeeper.Shard;
import net.xinshi.pigeon.common.Constants;

import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.resourcelock.IResourceLock;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-13
 * Time: 下午5:59
 * To change this template use File | Settings | File Templates.
 */

public class DistributedNettyLock implements IResourceLock {

    String type = "lock";

    long LOCK_WAIT = 1000 * 3600;
    long UNLOCK_WAIT = 1000 * 15;

    private NodesDispatcher nodesDispatcher = null;

    Logger logger = Logger.getLogger(DistributedNettyLock.class.getName());

    public DistributedNettyLock(NodesDispatcher nodesDispatcher) {
        this.nodesDispatcher = nodesDispatcher;
    }

    public void init() throws Exception {
        System.out.println("distributed flexobject init ...... ");
    }

    public void Lock(String resId) throws Exception {
        String threadId = IdentityKeygen.get();
        String lockid = "lock$" + resId + "$" + threadId;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, lockid);
        PigeonFuture pf = nodesDispatcher.commitAsync(type, resId, out);
        if (pf == null) {
            throw new Exception("Lock PigeonFuture == null , resid = " + resId);
        }
        pf.waitme(LOCK_WAIT);
        if (pf.isComplete()) {
            String msg = new String(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH, "UTF-8");
            String[] parts = msg.split("\\$");
            String action = parts[0];
            if (StringUtils.equals(action, "locked")) {
                return;
            }
        }
        throw new Exception("Lock wait time out , resid = " + resId);
    }

    public void Unlock(String resId) throws Exception {
        String threadId = IdentityKeygen.get();
        String lockid = "unlock$" + resId + "$" + threadId;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, lockid);
        PigeonFuture pf = nodesDispatcher.commitAsync(type, resId, out);
        if (pf == null) {
            throw new Exception("UnLock PigeonFuture == null , resid = " + resId);
        }
        pf.waitme(UNLOCK_WAIT);
        if (pf.isComplete()) {
            String msg = new String(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH, "UTF-8");
            String[] parts = msg.split("\\$");
            String action = parts[0];
            if (StringUtils.equals(action, "unlocked")) {
                return;
            }
        }
        throw new Exception("UnLock error , resid = " + resId);
    }

    private JSONObject readOneResLock(InputStream in) throws Exception {
        JSONObject jResLock = new JSONObject();
        String resId = CommonTools.readString(in);
        jResLock.put("resId",resId);
        JSONArray jlocks = new JSONArray();
        jResLock.put("locks",jlocks);
        long lockNum = CommonTools.readLong(in);
        for(int i=0; i<lockNum; i++){
            JSONObject jlock = new JSONObject();
            String lockKey = CommonTools.readString(in);
            String[] parts = lockKey.split("\\$");
            String remoteIp = parts[0];
            String threadId = parts[1];
            jlock.put("remoteIp",remoteIp);
            jlock.put("threadId",threadId);
            jlocks.put(jlock);
        }
        return jResLock;
    }

    public JSONArray reportLocks() throws Exception{
        String lockid = "reportLocks$null$null";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out, lockid);
        List<Shard> shards = nodesDispatcher.getClusterByTypeCode(PigeonNode.TYPE_CODE_LOCK);

        JSONArray jresLocks = new JSONArray();
        for(Shard shard : shards){
            PigeonNode node = shard.getMaster();
            InputStream in = nodesDispatcher.commitToNode(node.getTypeCode(),node,out);
            //读取所有locks的信息
            long resNum  =  CommonTools.readLong(in);
            for(int i=0; i<resNum;i++){
                JSONObject jresLock = readOneResLock(in);
                jresLocks.put(jresLock);
            }
        }
        return jresLocks;
    }

    public void stop() throws Exception {
        System.out.println("DistributedNettyLock do stop ... ");
    }

}

