package net.xinshi.pigeon.persistence;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-1
 * Time: 上午9:21
 * To change this template use File | Settings | File Templates.
 */

public interface IPigeonPersistence {

    long getVersion();

    void syncVersion(long begin, long end) throws Exception;

}
