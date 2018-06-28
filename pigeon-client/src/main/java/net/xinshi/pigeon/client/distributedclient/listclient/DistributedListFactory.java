package net.xinshi.pigeon.client.distributedclient.listclient;

import net.xinshi.pigeon.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon.util.IdChecker;
import net.xinshi.pigeon.list.IListFactory;
import net.xinshi.pigeon.list.ISortList;

import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-7
 * Time: 下午2:33
 * To change this template use File | Settings | File Templates.
 */

public class DistributedListFactory implements IListFactory {

    private NodesDispatcher nodesDispatcher = null;
    Logger logger = Logger.getLogger(DistributedListFactory.class.getName());

    public DistributedListFactory(NodesDispatcher nodesDispatcher) {
        this.nodesDispatcher = nodesDispatcher;
    }

    public ISortList getList(String listId, boolean create) throws Exception {
        IdChecker.assertValidId(listId);
        DistributedSortList sortList = new DistributedSortList();
        sortList.setListId(listId);
        sortList.setNodesDispatcher(nodesDispatcher);
        return sortList;
    }

    public ISortList createList(String listId) throws Exception {
        throw new Exception("not implemented.");
    }

    public void init() throws Exception {
        System.out.println("DistributedListFactory DO INIT ...... ");
    }

    public void set_state_word(int state_word) throws Exception {
        System.out.println("DistributedListFactory DO set_state_word ...... ");
    }

    @Override
    public long getLastTxid() throws Exception {
        return 0;
    }

}

