package net.xinshi.saas;

import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.SortListObject;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-8-20
 * Time: 下午4:41
 * To change this template use File | Settings | File Templates.
 */

public class MerchantListFactory extends SaasMerchant implements IListFactory {

    IListFactory rawListFactory;

    public MerchantListFactory(String merchantId, IListFactory listFactory, IListFactory rawListFactory) {
        super(merchantId, listFactory);
        this.rawListFactory = rawListFactory;
    }

    void adjustMetaList(String name) throws Exception {
        if (name == null || !SaasPigeonEngine.isRecordMetaList()) {
            return;
        }
        listFactory.getList(merchantId + "::list", true).add(new SortListObject(name, name));
    }

    @Override
    public ISortList getList(String listId, boolean create) throws Exception {
        if (create) {
            adjustMetaList(listId);
        }
        return rawListFactory.getList(getKey(listId), create);
    }

    @Override
    public ISortList createList(String listId) throws Exception {
        adjustMetaList(listId);
        return rawListFactory.createList(getKey(listId));
    }

    @Override
    public void init() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void set_state_word(int state_word) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getLastTxid() throws Exception {
        return rawListFactory.getLastTxid();
    }

}

