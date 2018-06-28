package net.xinshi.pigeon.server.distributedserver.lockserver;

import net.xinshi.pigeon.netty.server.IServerHandler;
import net.xinshi.pigeon.server.distributedserver.ServerConstants;
import net.xinshi.pigeon.server.distributedserver.ServerConfig;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-13
 * Time: 上午10:32
 * To change this template use File | Settings | File Templates.
 */

public class NettyLockServerHandler implements IServerHandler {

    ServerConfig sc;
    Log log = LogFactory.getLog(NettyLockServerHandler.class);
    protected static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected String startTime;


    class Locker {
        public int sequence = 0;
        public short flag = 0;
        public String ResID = null;
        public String remoteIP = null;
        public String threadID = null;
        public Channel channel = null;
        public int reference = 0;
    }

    Object mutex = new Object();
    HashMap<String, LinkedHashMap<String, Locker>> mapResLockers;
    HashMap<Channel, HashSet<Locker>> mapChannelLockers;

    public Map getStatusMap() {
        Map<String, String> mapStatus = new HashMap<String, String>();
        String info = "";
        info += "Resource count : " + mapResLockers.size();
        info += " Channel count : " + mapChannelLockers.size();
        mapStatus.put("resource_string", info);
        mapStatus.put("start_time", startTime);
        mapStatus.put("type", sc.getType());
        mapStatus.put("instance_name", sc.getInstanceName());
        mapStatus.put("name", sc.getName());
        mapStatus.put("node_name", sc.getNodeName());
        return mapStatus;
    }

    public NettyLockServerHandler(ServerConfig sc) {
        this.sc = sc;
        this.startTime = this.dateFormat.format(new Date());
        this.mapResLockers = new HashMap<String, LinkedHashMap<String, Locker>>();
        this.mapChannelLockers = new HashMap<Channel, HashSet<Locker>>();
    }

    public void init() throws Exception {
        System.out.println("NettyLockServerHandler do INIT ...... ");
    }

    @Override
    public void channelConnected(Channel ch) throws Exception {
    }

    public void cleanChannelLockers(Channel ch) throws Exception {
        Locker[] lockers = null;
        synchronized (mutex) {
            HashMap<String, Locker> mapNotify = new HashMap<String, Locker>();
            HashSet<Locker> setLocker = mapChannelLockers.get(ch);
            if (setLocker == null) {
                return;
            }
            for (Iterator iter = setLocker.iterator(); iter.hasNext(); ) {
                Locker locker = (Locker) iter.next();
                if (mapNotify.get(locker.ResID) == locker) {
                    mapNotify.remove(locker.ResID);
                }
                LinkedHashMap<String, Locker> mapLocker = mapResLockers.get(locker.ResID);
                if (mapLocker != null) {
                    String tid = locker.threadID;
                    int lastIdx = tid.lastIndexOf("@");
                    String real_tid = tid;
                    if(lastIdx>-1){
                        real_tid = tid.substring(0,lastIdx);
                    }
                    String key = locker.remoteIP + ":" + real_tid;

                    Locker t = mapLocker.get(key);
                    if (t == null) {
                        continue;
                    }
                    boolean b = false;
                    if (mapLocker.values().toArray()[0] == t) {
                        b = true;
                    }
                    mapLocker.remove(key);
                    if(mapLocker.size()==0){
                        mapResLockers.remove(key);
                    }
                    if (b && mapLocker.values().toArray() != null && mapLocker.values().toArray().length > 0) {
                        mapNotify.put(locker.ResID, (Locker) mapLocker.values().toArray()[0]);
                    }
                }
            }
            setLocker.clear();
            mapChannelLockers.remove(ch);
            int n = mapNotify.values().toArray().length;
            if (n > 0) {
                lockers = new Locker[n];
                mapNotify.values().toArray(lockers);
            }
        }
        if (lockers != null) {
            notifyLockers(lockers);
        }
    }


    private List<LinkedHashMap<String, Locker>> copyResLockers(){
        ArrayList<LinkedHashMap<String, Locker>> listResLocks = new ArrayList();
        synchronized (mutex){
            for(Map.Entry<String,LinkedHashMap<String, Locker>> entry : mapResLockers.entrySet()){
                listResLocks.add(entry.getValue());
            }
            return listResLocks;
        }
    }


    public boolean doLock(String res, String ip, String tid, Channel ch, int sq, short flag) {
        synchronized (mutex) {
            LinkedHashMap<String, Locker> mapLocker = mapResLockers.get(res);
            if (mapLocker == null) {
                mapLocker = new LinkedHashMap<String, Locker>();
                mapResLockers.put(res, mapLocker);
            }

            int lastIdx = tid.lastIndexOf("@");
            String real_tid = tid;
            if(lastIdx>-1){
                real_tid = tid.substring(0,lastIdx);
            }
            String key = ip + ":" + real_tid;
            Locker locker = mapLocker.get(key);
            if (locker == null) {
                locker = new Locker();
                locker.ResID = res;
                locker.remoteIP = ip;
                locker.threadID = tid;
                locker.channel = ch;
                locker.reference = 0;
                locker.sequence = sq;
                locker.flag = flag;
                mapLocker.put(key, locker);
            }
            ++locker.reference;
            HashSet<Locker> setLocker = mapChannelLockers.get(locker.channel);
            if (setLocker == null) {
                setLocker = new HashSet<Locker>();
                mapChannelLockers.put(locker.channel, setLocker);
            }
            if (!setLocker.contains(locker)) {
                setLocker.add(locker);
            }
            if (mapLocker.values().toArray()[0] == locker) {
                return true;
            }
        }
        return false;
    }

    public Locker doUnLock(String res, String ip, String tid) {
        synchronized (mutex) {
            LinkedHashMap<String, Locker> mapLocker = mapResLockers.get(res);
            if (mapLocker == null) {
                return null;
            }

            int lastIdx = tid.lastIndexOf("@");
            String real_tid = tid;
            if(lastIdx>-1){
                real_tid = tid.substring(0,lastIdx);
            }
            String key = ip + ":" + real_tid;

            Locker locker = mapLocker.get(key);
            if (locker == null) {
                return null;
            }
            if (--locker.reference > 0) {
                return null;
            }
            mapLocker.remove(key);
            if(mapLocker.size()==0){
                mapResLockers.remove(res);
            }
            HashSet<Locker> setLocker = mapChannelLockers.get(locker.channel);
            if (setLocker != null) {
                setLocker.remove(locker);
            }
            if (mapLocker.size() > 0) {
                return (Locker) mapLocker.values().toArray()[0];
            }
        }
        return null;
    }

    public void notifyLockers(Locker[] lockers) {
        for (int i = 0; i < lockers.length; i++) {
            Locker locker = lockers[i];
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                String info = "locked$" + locker.ResID + "$" + locker.threadID;
                out.write(info.getBytes());
                synchronized (locker.channel) {
                    ChannelFuture cf = locker.channel.write(Tools.buildChannelBuffer(locker.sequence, locker.flag, out));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public ChannelBuffer handler(Channel ch, byte[] buffer) {
        try {
            int sq = CommonTools.bytes2intJAVA(buffer, 4);
            short flag = CommonTools.bytes2shortJAVA(buffer, 8);
            InputStream is = new ByteArrayInputStream(buffer, ServerConstants.PACKET_PREFIX_LENGTH, buffer.length - ServerConstants.PACKET_PREFIX_LENGTH);
            String message = CommonTools.readString(is);
            String[] parts = message.split("\\$");
            String action = parts[0];
            String resId = parts[1];
            String threadId = parts[2];
            String ip = ch.getRemoteAddress().toString();
            if (ip.startsWith("/")) {
                ip = ip.substring(1);
            }
            int sp = ip.indexOf(":");
            if (sp > 0) {
                ip = ip.substring(0, sp);
            }
            if (StringUtils.equals(action, "lock")) {
                if (doLock(resId, ip, threadId, ch, sq, flag)) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    String info = "locked$" + resId + "$" + threadId;
                    out.write(info.getBytes());
                    return Tools.buildChannelBuffer(sq, flag, out);
                }
            } else if (StringUtils.equals(action, "unlock")) {
                Locker locker = doUnLock(resId, ip, threadId);
                if (locker != null) {
                    Locker[] lockers = new Locker[]{locker};
                    notifyLockers(lockers);
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                String info = "unlocked$" + resId + "$" + threadId;
                out.write(info.getBytes());
                return Tools.buildChannelBuffer(sq, flag, out);
            } else if (StringUtils.equals(action, "reportLocks")) {
                List<LinkedHashMap<String, Locker>> lockers = copyResLockers();

                ByteArrayOutputStream out = new ByteArrayOutputStream();

                //ResNum
                CommonTools.writeLong(out,lockers.size());

                for(LinkedHashMap<String, Locker> resLockQue : lockers){
                    int n = 0;
                    for(Map.Entry<String,Locker> entry:resLockQue.entrySet()){
                        Locker locker = entry.getValue();
                        if(n == 0){
                            //write resId only once
                           CommonTools.writeString(out,locker.ResID);
                           CommonTools.writeLong(out,resLockQue.size());
                        }
                        n += 1;
                        String key = locker.remoteIP + "$" + locker.threadID;
                        CommonTools.writeString(out,key);
                    }
                }
                return Tools.buildChannelBuffer(sq, flag, out);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void channelClosed(Channel ch) throws Exception {
        cleanChannelLockers(ch);
    }

}

