package net.xinshi.pigeon.adapter.impl;

import net.xinshi.pigeon.adapter.IPigeonEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon50.client.distributedclient.atomclient.DistributedAtom;
import net.xinshi.pigeon50.client.distributedclient.fileclient.AliyunOssClient;
import net.xinshi.pigeon50.client.distributedclient.fileclient.NettyFileClient;
import net.xinshi.pigeon50.client.distributedclient.flexobjectclient.DistributedFlexObjectFactory;
import net.xinshi.pigeon50.client.distributedclient.idclient.DistributedIdGenerator;
import net.xinshi.pigeon50.client.distributedclient.listclient.DistributedListFactory;
import net.xinshi.pigeon50.client.distributedclient.lockclient.DistributedNettyLock;
import net.xinshi.pigeon50.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon.filesystem.IFileSystem;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.util.logging.Logger;

public class ZooKeeperPigeonEngine implements IPigeonEngine {

    Logger logger = Logger.getLogger("ZooKeeperPigeonEngine");

    private NodesDispatcher nodesDispatcher = null;
    DistributedFlexObjectFactory flexobjectFactory;
    DistributedListFactory listFactory;
    DistributedAtom atom;
    DistributedIdGenerator idGenerator;
    DistributedNettyLock lock;
    IFileSystem fileSystem;
//    Logger logger = Logger.getLogger(ZooKeeperPigeonEngine.class.getName());

    public ZooKeeperPigeonEngine(String connectString, String podPath) throws Exception {
        nodesDispatcher = new NodesDispatcher();
        nodesDispatcher.init(connectString,podPath);
        this.flexobjectFactory = new DistributedFlexObjectFactory(this.nodesDispatcher);
        this.flexobjectFactory.init();
        this.listFactory = new DistributedListFactory(this.nodesDispatcher);
        this.listFactory.init();
        this.atom = new DistributedAtom(this.nodesDispatcher);
        this.atom.init();
        this.lock = new DistributedNettyLock(this.nodesDispatcher);
        this.lock.init();
        long idNumPerRound = 10000L;
        this.idGenerator = new DistributedIdGenerator(this.nodesDispatcher, idNumPerRound);
        this.idGenerator.init();


        ZooKeeper zk = new ZooKeeper(connectString, 1000, null);
        Stat stat = zk.exists(podPath + "/file_cluster",null);
        if(stat!=null){
            //not exists

            Stat stat1 = new Stat();
            byte[] data = zk.getData(podPath + "/file_cluster", false, stat1);
            String s = new String(data, "utf-8");
            System.out.printf(s);
            JSONObject jsonObject = new JSONObject(s);
            boolean enableAliyun = jsonObject.optBoolean("enableAliyun");
            if(enableAliyun){

                String apiEndPoint = jsonObject.optString("apiEndPoint");
                String internalUrlPrefix = jsonObject.optString("internalUrlPrefix");
                String externalUrlPrefix = jsonObject.optString("externalUrlPrefix");
                String accessKeyId = jsonObject.optString("accessKeyId");
                String accessKeySecret = jsonObject.optString("accessKeySecret");
                String bucketName = jsonObject.optString("bucketName");
                String includeList = jsonObject.optString("includeList");

                AliyunOssClient aliyunOssClient = new AliyunOssClient();
                aliyunOssClient.setApiEndPoint(apiEndPoint);
                aliyunOssClient.setInternalUrlPrefix(internalUrlPrefix);
                aliyunOssClient.setExternalUrlPrefix(externalUrlPrefix);
                aliyunOssClient.setAccessKeyId(accessKeyId);
                aliyunOssClient.setAccessKeySecret(accessKeySecret);
                aliyunOssClient.setBucketName(bucketName);
                aliyunOssClient.setIncludeList(includeList);
                aliyunOssClient.setLocalFileSystem(this.fileSystem);
                aliyunOssClient.init();
                this.fileSystem = aliyunOssClient;
            }
        }
        else{
            this.fileSystem = new NettyFileClient(this.nodesDispatcher,connectString,podPath + "/file_cluster");
            this.fileSystem.init();
        }

        zk.close();
    }


    @Override
    public IFlexObjectFactory getFlexObjectFactory() {
        return flexobjectFactory;
    }

    @Override
    public IIntegerAtom getAtom() {
        return atom;
    }

    @Override
    public IListFactory getListFactory() {
        return listFactory;
    }

    @Override
    public IIDGenerator getIdGenerator() {
        return idGenerator;
    }

    @Override
    public IResourceLock getLock() {
        return lock;
    }

    @Override
    public void stop() throws InterruptedException {

    }
    @Override
    public IFileSystem getFileSystem() {
        return fileSystem;
    }
}
