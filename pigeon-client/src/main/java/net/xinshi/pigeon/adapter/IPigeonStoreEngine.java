package net.xinshi.pigeon.adapter;

import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;

public interface IPigeonStoreEngine {

    //一个PigeonStoreEngine,在pigeon40中，也可以叫做一个pod

    IFlexObjectFactory getFlexObjectFactory();
    IIntegerAtom getAtom();
    IListFactory getListFactory();
    IIDGenerator getIdGenerator();
    IResourceLock getLock();

    void stop() throws InterruptedException;
}
