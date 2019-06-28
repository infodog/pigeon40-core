package net.xinshi.pigeon50.client.net.xinshi.pigeon.client.zookeeper;

import net.xinshi.pigeon50.client.util.ZkUtil;
import net.xinshi.pigeon.common.Constants;
import net.xinshi.pigeon.netty.client.Client;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import net.xinshi.pigeon.util.DefaultHashGenerator;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;


class ServerChangeWatcher implements Watcher {

    Shard shard;
    ZooKeeper zk;

    synchronized public static void checkMasterForShard(ZooKeeper zk, Shard shard, List<String> newSvrs) throws InterruptedException, UnsupportedEncodingException, JSONException, KeeperException {
        List<String> svrs = newSvrs;
        Collections.sort(svrs);
        List<PigeonNode> origServers = shard.getServers();
        for (Iterator<PigeonNode> it = origServers.iterator(); it.hasNext(); ) {
            PigeonNode node = it.next();
            boolean found = false;

            String ephemeralServerName = node.getEphemeralServerName();
            for (String svrName : svrs) {
                if (ephemeralServerName.equals(svrName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                //不存在
                node.clean();
                it.remove();
            }
        }
        //2.然后增加新加入的server
        for (String svrName : svrs) {
            boolean found = false;
            for (PigeonNode node : origServers) {
                if (svrName.equals(node.getEphemeralServerName())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                //获取最新数据
                PigeonNode pnode = ZkUtil.getPigeonNode(zk, shard.getFullPath(), svrName);
                origServers.add(pnode);
            }
        }
        PigeonNode node = origServers.get(0);
        node.init_client();
    }

    ServerChangeWatcher(ZooKeeper zk, Shard shard) {
        this.shard = shard;
        this.zk = zk;
    }


    @Override
    public void process(WatchedEvent event) {
        try {
            List<String> svrs = zk.getChildren(shard.getFullPath(), this);
            checkMasterForShard(zk, shard, svrs);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}


public class NodesDispatcher {
    String zkConnectionString;
    String podPath;
    ZooKeeper zk;
    List<Shard> flexobjectShards;
    List<Shard> idShards;
    List<Shard> listShards;
    List<Shard> locksShards;
    List<Shard> atomShards;
    List<Shard> fileShards;

    Thread checkMasterThread;
    Logger logger  = Logger.getLogger(NodesDispatcher.class.getName());

    private void startCheckMasterThread() {
        checkMasterThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Shard shard : flexobjectShards) {
                        List<String> svrs = zk.getChildren(shard.getFullPath(), false);
                        ServerChangeWatcher.checkMasterForShard(zk, shard, svrs);
                    }
                    for (Shard shard : idShards) {
                        List<String> svrs = zk.getChildren(shard.getFullPath(), false);
                        ServerChangeWatcher.checkMasterForShard(zk, shard, svrs);
                    }
                    for (Shard shard : listShards) {
                        List<String> svrs = zk.getChildren(shard.getFullPath(), false);
                        ServerChangeWatcher.checkMasterForShard(zk, shard, svrs);
                    }
                    for (Shard shard : atomShards) {
                        List<String> svrs = zk.getChildren(shard.getFullPath(), false);
                        ServerChangeWatcher.checkMasterForShard(zk, shard, svrs);
                    }
                    for (Shard shard : locksShards) {
                        List<String> svrs = zk.getChildren(shard.getFullPath(), false);
                        ServerChangeWatcher.checkMasterForShard(zk, shard, svrs);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    List<Shard> initShards(String shardsPath) throws KeeperException, InterruptedException, UnsupportedEncodingException, JSONException {
        logger.info("initShards : " + shardsPath);
        List<Shard> shards = new ArrayList();
        List<String> shardNames = zk.getChildren(shardsPath, false);
        logger.info("shardNames:" + StringUtils.join(shardNames,","));
        for (String shardName : shardNames) {
            final Shard shard = new Shard();
            shard.setName(shardName);
            Stat stat = new Stat();
            String shardPath = shardsPath + "/" + shardName;
            shard.setFullPath(shardPath);
            byte[] data = zk.getData(shardPath, false, stat);
            String s = new String(data, "utf-8");
            JSONObject jsonObject = new JSONObject(s);
            int min = jsonObject.optInt("min");
            int max = jsonObject.optInt("max");
            shard.setMinHash(min);
            shard.setMaxHash(max);

            //获得pigeonNode
            List<String> servers = zk.getChildren(shardPath, new ServerChangeWatcher(zk, shard));
            logger.info("servers:" + StringUtils.join(servers,","));

            Collections.sort(servers);
            List<PigeonNode> pnodes = new ArrayList();
            for (String svrName : servers) {
                PigeonNode pnode = ZkUtil.getPigeonNode(zk, shardPath, svrName);
                pnodes.add(pnode);
            }

            logger.info("server size for " + shardPath + " is " + pnodes.size());
            shard.setServers(pnodes);

            //对于第一个server，我们将其作为head
            if(pnodes.size()>0) {
                PigeonNode phead = pnodes.get(0);
                logger.info("init_client for : " + phead.getShardFullPath() + ", " + phead.getServiceHost() + ":" + phead.getServicePort() );
                phead.init_client();

                shards.add(shard);
            }
        }
        return shards;
    }


    public void init(String zkConnectionString, String podPath) throws IOException, KeeperException, InterruptedException, JSONException {
        this.zkConnectionString = zkConnectionString;
        this.podPath = podPath;
        zk = new ZooKeeper(zkConnectionString, 1000, null);

        String flexObjectShardsPath = podPath + "/object_cluster";
        String listShardsPath = podPath + "/list_cluster";
        String idShardsPath = podPath + "/id_cluster";
        String atomShardsPath = podPath + "/atom_cluster";
        String lockShardsPath = podPath + "/locks_cluster";
        String fileShardsPath = podPath + "/file_cluster";

        this.flexobjectShards = initShards(flexObjectShardsPath);
        this.listShards = initShards(listShardsPath);
        this.idShards = initShards(idShardsPath);
        this.atomShards = initShards(atomShardsPath);
        this.locksShards = initShards(lockShardsPath);
        this.fileShards = initShards(fileShardsPath);
        startCheckMasterThread();


    }

    public List<Shard> getClusterByType(String type) {
        if (type.equals("flexobject")) {
            return flexobjectShards;
        } else if (type.equals("list")) {
            return listShards;
        } else if (type.equals("atom")) {
            return atomShards;
        } else if (type.equals("idserver")) {
            return idShards;
        } else if (type.equals("lock")) {
            return locksShards;
        } else if (type.equals("file")) {
            return fileShards;
        }
        return null;
    }

    private int getTypeCode(String type) {
        return PigeonNode.getTypeCode(type);
    }

    public List<Shard> getClusterByTypeCode(int typecode) {
        switch (typecode) {
            case PigeonNode.TYPE_CODE_FLEXOBJECT:
                return flexobjectShards;
            case PigeonNode.TYPE_CODE_LIST:
                return listShards;
            case PigeonNode.TYPE_CODE_IDSERVER:
                return idShards;
            case PigeonNode.TYPE_CODE_ATOM:
                return atomShards;
            case PigeonNode.TYPE_CODE_LOCK:
                return locksShards;
            case PigeonNode.TYPE_CODE_FILE:
                return fileShards;
            default:
                return null;
        }
    }

    private Shard dispatchToShard(int typeCode, String key) throws Exception {
        int hash = DefaultHashGenerator.hash(key);
        PigeonNode pin = null;
        List<Shard> shards = getClusterByTypeCode(typeCode);
        if (shards != null) {
            for (Shard shard : shards) {
                if (hash >= shard.getMinHash() && hash <= shard.getMaxHash()) {
                    return shard;
                }
            }
        }
        return null;
    }

    private Shard dispatchToShard(String type, String key) throws Exception {
        int typeCode = getTypeCode(type);
        return dispatchToShard(typeCode, key);
    }


    public PigeonNode getPigeonNode(int typeCode, String name) throws Exception{
        Shard shard = dispatchToShard(typeCode, name);
        if (shard.getState() != Shard.SHARD_OK) {
            throw new Exception("Shard is changing server!");
        }
        if (shard.getServers().size() == 0) {
            throw new Exception("shard has no live server,the shard has down");
        }
        PigeonNode head = shard.getServers().get(0);
        head.setShardName(shard.getName());
        return head;
    }

    public PigeonFuture commitAsync(int typeCode, String name, ByteArrayOutputStream out) throws Exception {
//        Shard shard = dispatchToShard(typeCode, name);
//        if (shard.getState() != Shard.SHARD_OK) {
//            throw new Exception("Shard is changing server!");
//        }
//        if (shard.getServers().size() == 0) {
//            throw new Exception("shard has no live server,the shard has down");
//        }
        PigeonNode head = getPigeonNode(typeCode,name);
        Client client = head.client;

        int t = typeCode;

        try {
            int flag = ((t << 8) | head.getNo()) & 0xFFFF;
            PigeonFuture pf = client.send((short) flag, out.toByteArray());
            if (pf == null) {
                String detail = "[" + head.getServiceHost() + ":" + head.getServicePort() + "/" + head.getInstanceName() + " not ok.]";
                throw new Exception("netty commit server timeout " + detail);
            }
            return pf;
        } catch (Exception e) {
            String detail = "[" + head.getServiceHost() + ":" + head.getServicePort() + "/" + head.getInstanceName() + " not ok.]";
            throw new Exception("netty commit pf == null " + detail);
        }
    }

    public PigeonFuture commitAsync(String type, String name, ByteArrayOutputStream out) throws Exception {
        int typeCode = getTypeCode(type);
        return commitAsync(typeCode, name, out);
    }

    public InputStream commitToNode(int typeCode, PigeonNode node, ByteArrayOutputStream out) throws Exception {
        PigeonNode head = node;
        Client client = head.client;
        int t = typeCode;
        try {
            int flag = ((t << 8) | head.getNo()) & 0xFFFF;
            PigeonFuture pf = client.send((short) flag, out.toByteArray());
            if (pf == null) {
                String detail = "[" + head.getServiceHost() + ":" + head.getServicePort() + "/" + head.getInstanceName() + " not ok.]";
                throw new Exception("netty commit server timeout " + detail);
            } else {//if (pf != null) {
                boolean ok = false;
                ok = pf.waitme(1000 * 60);
                if (ok) {
                    return new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
                } else {
                    String detail = "[" + head.getServiceHost() + ":" + head.getServicePort() + "/" + head.getInstanceName() + " not ok.]";
                    throw new Exception("netty commit pf == null " + detail);
                }

            }
        } catch (Exception e) {
            String detail = "[" + head.getServiceHost() + ":" + head.getServicePort() + "/" + head.getInstanceName() + " , ]" + "typeCode : [" + typeCode + "]";
            throw new Exception("netty commit pf == null " + detail);
        }
    }

    public InputStream commitToNode(String type, PigeonNode node, ByteArrayOutputStream out) throws Exception {
        int t = getTypeCode(type);
        return commitToNode(t, node, out);

    }

    public InputStream commit(int typeCode, String name, ByteArrayOutputStream out) throws Exception {
        PigeonFuture pf = commitAsync(typeCode, name, out);
        boolean ok = false;

        if (pf != null) {
            ok = pf.waitme(1000 * 60);
            if (ok) {
                return new ByteArrayInputStream(pf.getData(), Constants.PACKET_PREFIX_LENGTH, pf.getData().length - Constants.PACKET_PREFIX_LENGTH);
            } else {
                Shard shard = dispatchToShard(typeCode, name);
                String detail = "data : [typecode:" + typeCode + ":" + name + ",length=" + out.size() + ",shard=" + shard.getFullPath()  + "]";
                throw new Exception("netty commit timeout:" + detail);
            }
        }
        return null;
    }

    public InputStream commit(String type, String name, ByteArrayOutputStream out) throws Exception {
        int typeCode = getTypeCode(type);
        return commit(typeCode, name, out);
    }


}
