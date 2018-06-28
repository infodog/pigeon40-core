package net.xinshi.pigeon.server.distributedserver.lockserver;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午5:44
 * To change this template use File | Settings | File Templates.
 */

public interface IAsynLockServer {

    boolean lock(String resId, String threadId) throws Exception;

    boolean unlock(String resId, String threadId, String[] nextThreadId) throws Exception;

    boolean removeWaitingThread(String resId, String threadId) throws Exception;

    public String getLocks() throws Exception;

}

