package net.xinshi.pigeon.list;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-11-14
 * Time: 1:32:34
 * To change this template use File | Settings | File Templates.
 */

public interface IListFactory {

    ISortList getList(String listId, boolean create) throws Exception;

    ISortList createList(String listId) throws Exception;

    void init() throws Exception;

    void set_state_word(int state_word) throws Exception;

    long getLastTxid() throws Exception;

}