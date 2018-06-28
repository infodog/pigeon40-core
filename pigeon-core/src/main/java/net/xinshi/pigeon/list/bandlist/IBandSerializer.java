package net.xinshi.pigeon.list.bandlist;

import net.xinshi.pigeon.list.bandlist.bean.Band;

import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-11-29
 * Time: 8:35:32
 * To change this template use File | Settings | File Templates.
 */

public interface IBandSerializer {

    Object serialize(Collection<Band> bands) throws Exception;

    List<Band> unserializeBandList(Object input) throws Exception;

    Band unserializeBand(Object input) throws Exception;

    void parseValue(Band band) throws Exception;

}

