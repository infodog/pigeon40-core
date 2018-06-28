package net.xinshi.pigeon.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-20
 * Time: 上午10:12
 * To change this template use File | Settings | File Templates.
 */

public class LRUMap<K, V> extends LinkedHashMap<K, V> {

    int maxEntries = 1 << 30;

    public LRUMap(int maxEntries) {
        super(10000, 0.75f, true);
        this.maxEntries = maxEntries;
    }

    protected boolean removeEldestEntry(Map.Entry eldest) {
        if (this.size() > maxEntries) {
            return true;
        }
        return false;
    }

}
