package net.xinshi.pigeon40.saas;


import net.xinshi.pigeon.atom.IIntegerAtom;
import net.xinshi.pigeon.filesystem.IFileSystem;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.resourcelock.IResourceLock;
import net.xinshi.pigeon40.adapter.IPigeonEngine;

import java.util.HashMap;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-20
 * Time: 下午2:54
 * To change this template use File | Settings | File Templates.
 */

public class SaasPigeonEngine implements IPigeonEngine {

    IPigeonEngine rawPigeonEngine;

    public static boolean recordMetaList = true;

    IListFactory metaListFactory = null;

    ThreadLocal merchant = new ThreadLocal();
    HashMap merchantAtom = new HashMap();
    HashMap merchantObjs = new HashMap();
    HashMap merchantList = new HashMap();
    HashMap merchantLock = new HashMap();

    synchronized void checkMetaListFactory() {
        if (metaListFactory == null) {
            recordMetaList = false;
        }
    }

    public IPigeonEngine getRawPigeonEngine() {
        return rawPigeonEngine;
    }

    public void setRawPigeonEngine(IPigeonEngine rawPigeonEngine) {
        this.rawPigeonEngine = rawPigeonEngine;
    }

    public static boolean isRecordMetaList() {
        return recordMetaList;
    }

    public static void setRecordMetaList(boolean recordMetaList) {
        SaasPigeonEngine.recordMetaList = recordMetaList;
    }

    public IListFactory getMetaListFactory() {
        return metaListFactory;
    }

    public void setMetaListFactory(IListFactory metaListFactory) {
        this.metaListFactory = metaListFactory;
    }

    public void setCurrentMerchantId(String merchantId) {
        merchant.set(merchantId);
    }

    public String getCurrentMerchantId() {
        return (String) merchant.get();
    }

    @Override
    public IFileSystem getFileSystem() {
        return rawPigeonEngine.getFileSystem();
    }

    @Override
    public synchronized IFlexObjectFactory getFlexObjectFactory() {
        Object id = merchant.get();
        if (id == null) {
            return null;
        }
        Object obj = merchantObjs.get(id);
        if (obj == null) {
            checkMetaListFactory();
            obj = new MerchantFlexObjectFactory((String) id, metaListFactory, rawPigeonEngine.getFlexObjectFactory());
            merchantObjs.put(id, obj);
        }
        return (MerchantFlexObjectFactory) obj;
    }

    @Override
    public synchronized IIntegerAtom getAtom() {
        Object id = merchant.get();
        if (id == null) {
            return null;
        }
        Object obj = merchantAtom.get(id);
        if (obj == null) {
            checkMetaListFactory();
            obj = new MerchantAtom((String) id, metaListFactory, rawPigeonEngine.getAtom());
            merchantAtom.put(id, obj);
        }
        return (MerchantAtom) obj;
    }

    @Override
    public synchronized IListFactory getListFactory() {
        Object id = merchant.get();
        if (id == null) {
            return null;
        }
        Object obj = merchantList.get(id);
        if (obj == null) {
            checkMetaListFactory();
            obj = new MerchantListFactory((String) id, metaListFactory, rawPigeonEngine.getListFactory());
            merchantList.put(id, obj);
        }
        return (MerchantListFactory) obj;
    }

    @Override
    public synchronized IIDGenerator getIdGenerator() {
        return rawPigeonEngine.getIdGenerator();
    }

    @Override
    public synchronized IResourceLock getLock() {
        Object id = merchant.get();
        if (id == null) {
            return null;
        }
        Object obj = merchantLock.get(id);
        if (obj == null) {
            checkMetaListFactory();
            obj = new MerchantResourceLock((String) id, rawPigeonEngine.getLock());
            merchantLock.put(id, obj);
        }
        return (MerchantResourceLock) obj;
    }

    @Override
    public void stop() throws InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

}

