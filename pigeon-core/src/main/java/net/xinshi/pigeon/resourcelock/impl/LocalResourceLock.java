package net.xinshi.pigeon.resourcelock.impl;

import net.xinshi.pigeon.resourcelock.IResourceLock;
import org.json.JSONArray;

import java.util.Hashtable;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 2010-5-3
 * Time: 11:30:06
 * To change this template use File | Settings | File Templates.
 */
public class LocalResourceLock implements IResourceLock {
    private class Lock {
        int count;
        ReentrantLock theLock;
        public Lock(){
            count = 0;
            theLock = new ReentrantLock();
        }
    }

    int maxLocks;

    public int getMaxLocks() {
        return maxLocks;
    }

    public void setMaxLocks(int maxLocks) {
        this.maxLocks = maxLocks;
    }

    Hashtable locksMap = new Hashtable();

    synchronized Lock getLock(String resId) {
        Lock lock = (Lock) locksMap.get(resId);
        if (lock == null) {
           lock = new Lock();
           lock.count++;
           locksMap.put(resId,lock);
        }
        else{
            lock.count++;
        }
        return lock;

    }

    public void Lock(String resId) throws Exception {
        Lock lock = (Lock)getLock(resId);
        lock.theLock.lock();
    }

    synchronized public void Unlock(String resId) throws Exception {
        Lock lock = (Lock) locksMap.get(resId);
        if(lock==null){
            throw new Exception("lock is null while unlock.");
        }
        lock.count--;
        lock.theLock.unlock();
        if(lock.count==0){
            locksMap.remove(resId);
        }
    }

    @Override
    public JSONArray reportLocks() throws Exception {
        throw new Exception("not implemented.");
    }
}
