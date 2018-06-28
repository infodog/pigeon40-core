package net.xinshi.pigeon.server.distributedserver.lockserver;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:43
 * To change this template use File | Settings | File Templates.
 */

public class AsynLockServer implements IAsynLockServer {

    ConcurrentHashMap<String, Lock> locks = new ConcurrentHashMap();

    public Lock getLock(String resId) throws Exception {
        synchronized (locks) {
            Lock lock = locks.get(resId);
            if (lock == null) {
                lock = new Lock(resId);
                locks.put(resId, lock);
            }
            return lock;
        }
    }

    class Lock {
        String resId;
        String threadId;
        List<String> waitingLocks;

        Lock(String resId) {
            this.resId = resId;
            waitingLocks = new Vector<String>();
        }
    }

    synchronized public boolean lock(String resId, String threadId) throws Exception {
        Lock lock = getLock(resId);
        synchronized (lock) {
            if (lock.threadId == null) {
                lock.threadId = threadId;
                return true;
            }
            if (StringUtils.equals(lock.threadId, threadId)) {
                return true;
            } else {
                lock.waitingLocks.add(threadId);
                return false;
            }
        }
    }

    synchronized public boolean unlock(String resId, String thread, String[] nextThreadId) throws Exception {
        Lock lock = getLock(resId);
        synchronized (lock) {
            if (!StringUtils.equals(lock.threadId, thread)) {
                return false;
            }
            if (lock.waitingLocks.size() > 0) {
                lock.threadId = (String) lock.waitingLocks.get(0);
                lock.waitingLocks.remove(0);
                nextThreadId[0] = lock.threadId;
                return true;

            } else {
                lock.threadId = null;
                synchronized (locks) {
                    locks.remove(lock.resId);
                }
                return true;
            }
        }
    }

    synchronized public boolean removeWaitingThread(String resId, String threadId) throws Exception {
        Lock lock = getLock(resId);
        if (lock.waitingLocks != null) {
            for (Iterator it = lock.waitingLocks.iterator(); it.hasNext();) {
                String tid = (String) it.next();
                if (StringUtils.equals(tid, threadId)) {
                    it.remove();
                }
            }
        }
        if (StringUtils.equals(lock.threadId, threadId)) {
            lock.threadId = null;
        }
        if (lock.threadId == null && lock.waitingLocks.size() == 0) {
            locks.remove(resId);
        }
        return true;
    }

    public String getLocks() throws Exception {
        JSONObject json = new JSONObject();
        JSONArray array = new JSONArray();
        json.put("locks", array);
        synchronized (locks) {
            for (Lock lock : this.locks.values()) {
                JSONObject item = new JSONObject();
                item.put("resId", lock.resId);
                item.put("threadId", lock.threadId);
                item.put("isLocked", true);
                array.put(item);
                for (String wait : lock.waitingLocks) {
                    item = new JSONObject();
                    item.put("resId", lock.resId);
                    item.put("threadId", wait);
                    item.put("isLocked", false);
                    array.put(item);
                }
            }
        }
        return json.toString();
    }
}

