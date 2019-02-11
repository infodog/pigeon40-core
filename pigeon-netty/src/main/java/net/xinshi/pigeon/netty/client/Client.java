package net.xinshi.pigeon.netty.client;

import net.xinshi.pigeon.netty.common.PigeonDecoder;
import net.xinshi.pigeon.netty.common.PigeonFuture;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.jboss.netty.channel.Channels.pipeline;

public class Client extends Thread implements Comparable<Client> {

    static  Logger logger = Logger.getLogger(Client.class.getName());

    private String host;
    private int port;
    private int conns = 2;
    private volatile int sequence = 0;
    private volatile int index = 0;
    private ClientBootstrap bootstrap = null;
//    private Vector<Channel> vecChannels = new Vector<Channel>();
    private LinkedHashMap<Integer, PigeonFuture> mapFutures = new LinkedHashMap<Integer, PigeonFuture>();
    private boolean startup = false;
    private LinkedBlockingQueue<Channel> freeChannels = new LinkedBlockingQueue();
    private LinkedBlockingQueue<Channel> connectedChannels = new LinkedBlockingQueue();


    public boolean isStartup() {
        return startup;
    }

    public void setStartup(boolean startup) {
        this.startup = startup;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getConns() {
        return conns;
    }

    public void setConns(int conns) {
        this.conns = conns;
    }

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Client(String host, int port, int conns) {
        this.host = host;
        this.port = port;
        this.conns = conns;
    }


    void makeConnection(){
        Channel channel = null;
        logger.info("connecting to " + host + ":" + port);
        ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(host, port));

        while(true){
            connectFuture.awaitUninterruptibly();
            if(connectFuture.isDone()){
                channel = connectFuture.getChannel();
                if(channel.isOpen()){
                    connectedChannels.add(channel);
                    freeChannels.add(channel);
//                    vecChannels.add(channel);
                    logger.log(Level.INFO,"channel connected,channel id is " + channel.getId());
                }

                break;
            }
            else{
                logger.info("server not online " + host + ":" + port );
            }
        }
    }

    private boolean init_channels() {
        synchronized (connectedChannels) {
            for (Channel ch : connectedChannels) {
                ch.close();
            }
            freeChannels.clear();
            connectedChannels.clear();
            for (int i = 0; i < conns; i++) {
                try {
                    makeConnection();
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.log(Level.INFO,"error while connecting to pigeon server." + e.getMessage());
                }

            }
            if (connectedChannels.size() == conns) {
                return true;
            }
            if(connectedChannels.size()>0){
                logger.warning("try to make " + conns + "connections, succeeded connections were " + connectedChannels.size());
                return true;
            }
            return false;
        }
    }

    public boolean init() {
        synchronized (connectedChannels) {
            try {
                bootstrap = new ClientBootstrap(
                        new NioClientSocketChannelFactory(
                                Executors.newCachedThreadPool(),
                                Executors.newCachedThreadPool()));
                final ExecutionHandler eh = new ExecutionHandler(Executors.newFixedThreadPool(conns));
                final Client me = this;
                bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                    public ChannelPipeline getPipeline() throws Exception {
                        ChannelPipeline cpl = pipeline(new PigeonDecoder(), eh, new ClientHandler(me));
                        return cpl;
                    }
                });
                if (!init_channels()) {
                    System.out.println("netty init channels - server not online ... ");
                }
            } catch (Exception e) {
                e.printStackTrace();
                connectedChannels.clear();
                return false;
            }
            this.start();
            // System.out.println("netty client init ... ");
            startup = true;
            return true;
        }
    }

    public synchronized int getSequence() {
        ++sequence;
        if (sequence == Integer.MAX_VALUE) {
            sequence = 1;
        }
        return sequence;
    }




    public void returnFreeChannel(Channel ch){
        synchronized(connectedChannels){
            try{
                freeChannels.put(ch);
            }
            catch (Exception e){
                //should never happen;
                e.printStackTrace();
            }

        }
    }

    public Channel getFreeChannel(){
        try {
            while(true) {
                Channel ch = freeChannels.poll(1, TimeUnit.SECONDS);
                if(ch == null){
                    makeConnection();
                    continue;
                }
                if (ch!=null && !ch.isConnected()) {
                    logger.log(Level.INFO, "channel closed while client get it from cache.");
                    connectedChannels.remove(ch);
                }
                else if(ch!=null){
                    return ch;
                }
            }
        } catch (InterruptedException e) {
            //should never happen
            e.printStackTrace();
            return null;
        }
    }


    public void addPigeonFuture(Integer sq, PigeonFuture pf) {
        synchronized (mapFutures) {
            mapFutures.put(sq, pf);
        }
    }

    public void delPigeonFuture(Integer sq) {
        synchronized (mapFutures) {
            mapFutures.remove(sq);
        }
    }

    public void channelClosed(Channel ch) throws Exception {
        synchronized (freeChannels) {
            connectedChannels.remove(ch);
            freeChannels.remove(ch);

            synchronized (mapFutures) {
                for (PigeonFuture pf : mapFutures.values()) {
                    if (pf.getChannel() == ch) {
                        synchronized (pf) {
                            pf.setCancel(true);
                            pf.notify();
                        }
                    }
                }
            }
        }
        ch.close();
    }

    public PigeonFuture notifyPigeonFuture(Integer sq, short flag, byte[] data) {
        PigeonFuture pf;
        synchronized (mapFutures) {
            pf = mapFutures.get(sq);
        }
        if (pf != null) {
            pf.setFlag(flag);
            pf.setData(data);
            synchronized (pf) {
                pf.setComplete(true);
                pf.notify();
            }
        }
        return pf;
    }



    public PigeonFuture try_write(byte[] data,boolean bWriteFlag,short flag){

        Channel ch = null;
        boolean isChannelOk = true;
        try {
            ch = getFreeChannel();
            PigeonFuture pf = new PigeonFuture(this);
            pf.setChannel(ch);

            int sq = this.getSequence();
            pf.setSequence(sq);
            pf.setData(data);
            addPigeonFuture(sq, pf);
            ChannelBuffer dcb;
            if (bWriteFlag) {
                int len = data.length + 10;
                dcb = ChannelBuffers.dynamicBuffer(len);
                dcb.writeInt(len);
                dcb.writeInt(sq);
                dcb.writeShort(flag);
                dcb.writeBytes(data);
            }
            else{
                int len = data.length + 8;
                dcb = ChannelBuffers.dynamicBuffer(len);
                dcb.writeInt(len);
                dcb.writeInt(sq);
                dcb.writeBytes(data);
            }

            synchronized (ch) {
                try {
                    ChannelFuture cf = ch.write(dcb);
                    if (!cf.awaitUninterruptibly().isSuccess()) {
                        //没有写出去
                        return null;

                    }
                } catch (Exception e) {
//                e.printStackTrace();
                    return null;
                }
            }
            return pf;
        }
        catch(Exception e){
            isChannelOk = false;
            return null;

        }
        finally {
            if(isChannelOk && ch.isOpen()){
                logger.log(Level.FINEST,"return channel,channelId:" + ch.getId());
                returnFreeChannel(ch);
            }
            else{
                try {
                    logger.log(Level.SEVERE,"exception error while write data:" + ch .getId());
                    channelClosed(ch);
                }
                catch (Exception ex){
                }
            }
        }
    }

    public PigeonFuture send(byte[] data) {
        int tryTimes = 0;
        while(tryTimes<10){
            tryTimes++;
            PigeonFuture pf = try_write(data,false,(short)0);
            if(pf!=null){
                return pf;
            }
        }
        return null;
    }


    public PigeonFuture send(short flag, byte[] data) {
        int tryTimes = 0;
        while(tryTimes<10){
            tryTimes++;
            PigeonFuture pf = try_write(data,true,flag);
            if(pf!=null){
                return pf;
            }
        }
        return null;
    }



    public void run() {
        final long TIME_OUT_SECENDS = 1000 * 3600;
        Thread.currentThread().setName("Netty_Client_run");
        while (true) {
            try {
                long time = System.currentTimeMillis();
                LinkedList<Integer> listSQ = new LinkedList<Integer>();
                int ix = 0;
                int iy = 0;
                synchronized (mapFutures) {
                    ix = mapFutures.size();
                    for (Iterator it = mapFutures.values().iterator(); it.hasNext(); ) {
                        PigeonFuture pf = (PigeonFuture) it.next();
                        if (pf.getBornTime() + TIME_OUT_SECENDS < time) {
                            listSQ.add(pf.getSequence());
                        } else {
                            break;
                        }
                    }
                    for (Iterator<Integer> it = listSQ.listIterator(); it.hasNext(); ) {
                        Integer sq = it.next();
                        mapFutures.remove(sq);
                    }
                    iy = mapFutures.size();
                }
                if (ix > 100) {
                    System.out.println(Calendar.getInstance().getTime().toString() + " unprocessed packets : " + ix + " , amount which,timeout packets : " + iy);
                }
                Thread.sleep(TIME_OUT_SECENDS);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public InputStream Commit(short flag, ByteArrayOutputStream out) {
        InputStream in;
        try {
            PigeonFuture pf = send(flag, out.toByteArray());
            if (pf == null) {
                pf = send(flag, out.toByteArray());
            }
            boolean ok = false;
            try {
                if (pf != null) {
                    ok = pf.waitme(1000 * 60);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (pf == null) {
                throw new Exception("netty commit pf == null");
            }
            if (!ok) {
                throw new Exception("netty commit server timeout");
            }
            in = new ByteArrayInputStream(pf.getData(), 10, pf.getData().length - 10);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return in;
    }

    public int compareTo(Client o) {
        int rc = this.getHost().compareToIgnoreCase(o.getHost());
        if (rc == 0) {
            return this.getPort() - o.getPort();
        }
        return rc;
    }

    public void closeAll() {
        synchronized (connectedChannels) {
            for (Channel ch : connectedChannels) {
                ch.close();
            }
        }
    }
}
