package net.xinshi.pigeon.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-4-27
 * Time: 下午5:14
 * To change this template use File | Settings | File Templates.
 */

public class SoftHashMap extends ConcurrentHashMap {

    private class SoftReferenceContainKey extends SoftReference {
        Object _key;

        public SoftReferenceContainKey(Object key, Object referent, ReferenceQueue q) {
            super(referent, q);
            this._key = key;
        }
    }
    static int count = 0;
    ReferenceQueue queue;

    public SoftHashMap() {
        queue = new ReferenceQueue();
        (new Thread(new Runnable() {
            public void run() {
                long n = 0;
                Thread.currentThread().setName("SoftHashMap_run" + count++);
                while (true) {
                    try {
                        Object o = queue.remove(1000 * 3);
                        if (o != null) {
                            SoftReferenceContainKey srck = (SoftReferenceContainKey) o;
                            get(srck._key);
                            if (++n % 10000 == 0) {
                                System.out.println(TimeTools.getNowTimeString() + " SoftHashMap release key = " + n);
                            }
                            if (n < 0) {
                                n = 0;
                            }
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
        })).start();
    }

    public Object put(Object k, Object v) {
        SoftReferenceContainKey srck = new SoftReferenceContainKey(k, v, queue);
        SoftReferenceContainKey oldsrck = (SoftReferenceContainKey) super.put(k, srck);
        if (oldsrck == null) {
            return null;
        }
        return oldsrck.get();
    }

    public Object get(Object k) {
        SoftReferenceContainKey srck = (SoftReferenceContainKey) super.get(k);
        if (srck == null) {
            return null;
        }
        if (srck.get() == null) {
            super.remove(k);
            return null;
        }
        return srck.get();
    }

}
