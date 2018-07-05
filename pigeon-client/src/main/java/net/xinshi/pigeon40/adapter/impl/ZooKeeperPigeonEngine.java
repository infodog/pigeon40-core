package net.xinshi.pigeon40.adapter.impl;

import net.xinshi.pigeon40.adapter.IPigeonEngine;
import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon40.client.distributedclient.atomclient.DistributedAtom;
import net.xinshi.pigeon40.client.distributedclient.fileclient.NettyFileClient;
import net.xinshi.pigeon40.client.distributedclient.flexobjectclient.DistributedFlexObjectFactory;
import net.xinshi.pigeon40.client.distributedclient.idclient.DistributedIdGenerator;
import net.xinshi.pigeon40.client.distributedclient.listclient.DistributedListFactory;
import net.xinshi.pigeon40.client.distributedclient.lockclient.DistributedNettyLock;
import net.xinshi.pigeon40.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon.filesystem.IFileSystem;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;

import java.util.logging.Logger;

public class ZooKeeperPigeonEngine implements IPigeonEngine {

    private NodesDispatcher nodesDispatcher = null;
    DistributedFlexObjectFactory flexobjectFactory;
    DistributedListFactory listFactory;
    DistributedAtom atom;
    DistributedIdGenerator idGenerator;
    DistributedNettyLock lock;
    NettyFileClient fileSystem;
    Logger logger = Logger.getLogger(ZooKeeperPigeonEngine.class.getName());

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
        this.fileSystem = new NettyFileClient(this.nodesDispatcher,connectString,podPath + "/file_cluster");
        this.fileSystem.init();
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
