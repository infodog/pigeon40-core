package net.xinshi.pigeon.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: zxy
 * Date: 2009-12-20
 * Time: 0:46:17
 * To change this template use File | Settings | File Templates.
 */
public class LRUCache<K,V> extends LinkedHashMap<K,V> {
    int maxEntries;
    Object locker;

    public Object getLocker() {
        return locker;
    }

    public void setLocker(Object locker) {
        this.locker = locker;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    protected boolean removeEldestEntry(Map.Entry eldest) {

        if (this.size() > maxEntries) {
            return true;
        }
        return false;

    }
}
