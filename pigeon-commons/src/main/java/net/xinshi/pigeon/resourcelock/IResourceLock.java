package net.xinshi.pigeon.resourcelock;

import org.json.JSONArray;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 2010-5-3
 * Time: 11:23:23
 * To change this template use File | Settings | File Templates.
 */
public interface IResourceLock {
    void Lock(String resId) throws Exception;
    void Unlock(String resId) throws Exception;
    JSONArray reportLocks() throws Exception;
}
