package net.xinshi.pigeon.list.bandlist;

import net.xinshi.pigeon.list.bandlist.bean.Band;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-11-14
 * Time: 19:25:50
 * To change this template use File | Settings | File Templates.
 */

public interface IListBandDao {

    Band getHeadBand(String listName) throws Exception;

    Band getBandById(long bandid) throws Exception;

    void insertBand(Band band) throws Exception;

    void updateBand(Band band) throws Exception;

    boolean isExists(long bandid) throws Exception;

    void deleteById(long bandid) throws Exception;

}

