package net.xinshi.pigeon40.client.distributedclient.lockclient;



import org.apache.commons.lang.StringUtils;

import java.lang.management.ManagementFactory;
import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * User: zhengxiangyang
 * Date: 11-11-1
 * Time: 下午12:06
 * To change this template use File | Settings | File Templates.
 */
public class IdentityKeygen {
    private static ThreadLocal context = new ThreadLocal();
    private static ThreadLocal tickContext = new ThreadLocal();
    public static String get() {
        String uuid = (String) context.get();
        if (uuid == null) {
            uuid = UUID.randomUUID().toString();
            String threadName = Thread.currentThread().getName();
            String host = ManagementFactory.getRuntimeMXBean().getName();
            uuid = threadName + "@" + host + "@" + uuid;
            context.set(uuid);
        }
        String enableLockTick = System.getProperty("enableLockTick");
        Long tick = new Long(0);
        if(StringUtils.equals(enableLockTick,"true")){
            tick = (Long) tickContext.get();
            if(tick==null){
                tick = new Long(0);
                tickContext.set(tick);
            }
            else{
                tick = tick + 1;
                tickContext.set(tick);
            }
        }


        return uuid + "@" + tick;
//        return Thread.currentThread().getName();
    }
}
