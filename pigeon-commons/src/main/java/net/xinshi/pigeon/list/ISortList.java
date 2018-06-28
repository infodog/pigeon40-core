package net.xinshi.pigeon.list;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-11-22
 * Time: 14:52:34
 * To change this template use File | Settings | File Templates.
 */

public interface ISortList {

    List<SortListObject> getRange(int beginIndex, int number) throws Exception;

    boolean delete(SortListObject sortObj) throws Exception;

    boolean add(SortListObject sortObj) throws Exception;

    boolean add(List<SortListObject> listSortObj) throws Exception;

    boolean reorder(SortListObject oldObj, SortListObject newObj) throws Exception;

    boolean isExists(String key, String objid) throws Exception;

    public long getLessOrEqualPos(SortListObject obj) throws Exception;

    SortListObject getSortListObject(String key) throws Exception;

    List<SortListObject> getHigherOrEqual(SortListObject obj, int num) throws Exception;

    long getSize() throws Exception;

}
