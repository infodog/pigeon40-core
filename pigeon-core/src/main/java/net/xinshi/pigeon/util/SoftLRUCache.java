package net.xinshi.pigeon.util;

import java.lang.ref.SoftReference;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 12-1-31
 * Time: 下午2:49
 * To change this template use File | Settings | File Templates.
 */
public class SoftLRUCache extends LRUCache {


    public Object put(Object k, Object v) {
        SoftReference wref = new SoftReference(v);
        SoftReference oldref = (SoftReference) super.put(k, wref);
        if (oldref == null)
            return null;

        return oldref.get();
    }

    public Object get(Object k) {
        SoftReference ref = (SoftReference) super.get(k);
        if (ref == null)
            return null;

        if (ref.get() == null) {
            super.remove(k);
            return null;
        }
        return ref.get();
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        SoftReference ref = (SoftReference) eldest.getValue();
        if (ref == null) {
            return true;
        }
        if (ref.get() == null) {
            return true;
        } else {
            return false;
        }


    }

}
