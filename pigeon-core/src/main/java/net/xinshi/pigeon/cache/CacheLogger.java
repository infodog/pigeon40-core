package net.xinshi.pigeon.cache;

import net.xinshi.pigeon.util.SoftHashMap;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-2-27
 * Time: 上午10:11
 * To change this template use File | Settings | File Templates.
 */

public class CacheLogger {

    Map Cache;
    Map DirtyCache;
    Map SavingDirtyCache;
    Object notification = null;

    public String getCacheString() {
        return "Cache = " + Cache.size() + ", DirtyCache = " + DirtyCache.size() + ", SavingDirtyCache = " + SavingDirtyCache.size();
    }

    public Object getNotification() {
        return notification;
    }

    public void setNotification(Object notification) {
        this.notification = notification;
    }

    public Map getSavingDirtyCache() {
        return SavingDirtyCache;
    }

    public CacheLogger() {

        Cache = Collections.synchronizedMap(new SoftHashMap());
        DirtyCache = Collections.synchronizedMap(new ConcurrentHashMap());
        SavingDirtyCache = Collections.synchronizedMap(new ConcurrentHashMap());
    }

    public synchronized boolean noDirtyCache() {
        if (DirtyCache.size() == 0) {
            return true;
        }
        return false;
    }

    public synchronized boolean noSavingDirtyCache() {
        if (SavingDirtyCache.size() == 0) {
            return true;
        }
        return false;
    }



    public synchronized void swapSavingToCache() {
        for(Object o : SavingDirtyCache.entrySet()){
            Map.Entry entry = (Map.Entry) o;
            Cache.put(entry.getKey(),entry.getValue());
        }
        SavingDirtyCache.clear();
    }

    public synchronized Object getFromCaches(Object key) {
        Object obj;
        obj = DirtyCache.get(key);
        if (obj == null) {
            obj = SavingDirtyCache.get(key);
            if (obj == null) {
                obj = Cache.get(key);
            }
        }
        return obj;
    }

    public synchronized void putToCache(Object key, Object val) {
        Cache.put(key, val);
    }

    public synchronized void putToDirtyCache(Object key, Object val) {
        DirtyCache.put(key, val);
        if (notification != null) {
            synchronized (notification) {
                notification.notify();
            }
        }
    }

    public synchronized boolean swapToSaving() throws Exception {
        try {
            if (DirtyCache.size() == 0 || SavingDirtyCache.size() != 0) {
                return false;
            }
            SavingDirtyCache.putAll(DirtyCache);
            DirtyCache.clear();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public synchronized int getDirtyCacheSize() {
        return DirtyCache.size();
    }

    public synchronized int getSavingDirtyCacheSize() {
        return SavingDirtyCache.size();
    }

}

