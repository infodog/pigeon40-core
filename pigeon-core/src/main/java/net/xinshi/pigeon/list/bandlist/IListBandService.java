package net.xinshi.pigeon.list.bandlist;

import net.xinshi.pigeon.list.ISortList;
import net.xinshi.pigeon.list.bandlist.bean.Band;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-11-14
 * Time: 21:37:30
 * To change this template use File | Settings | File Templates.
 */

public interface IListBandService {

    public Band getBandById(ISortList list, long bandid) throws Exception;

    public Band getHeadBand(String listId) throws Exception;

    long writeLogAndDuplicate(String msg,long txid) throws Exception;

    int getMaxObjectsPerBand() throws Exception;

    int getMaxBandInfosPerBand() throws Exception;

    void putToDirtyBandList(Band tailBand) throws Exception;

    long getNewBandId() throws Exception;

    void setVersion(long version) throws Exception;

}

