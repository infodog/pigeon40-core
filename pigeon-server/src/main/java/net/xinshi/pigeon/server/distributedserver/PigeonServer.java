package net.xinshi.pigeon.server.distributedserver;

import net.xinshi.pigeon.common.Constants;
import net.xinshi.pigeon.netty.common.PigeonDecoder;
import net.xinshi.pigeon.netty.server.IServerHandler;
import net.xinshi.pigeon.netty.server.ServerHandler;
import net.xinshi.pigeon.server.distributedserver.atomserver.AtomServer;
import net.xinshi.pigeon.server.distributedserver.atomserver.NettyAtomServerHandler;
import net.xinshi.pigeon.server.distributedserver.control.ServerCommand;
import net.xinshi.pigeon.server.distributedserver.fileserver.FileServer;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.FlexObjectServer;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.NettyFlexObjectServerHandler;
import net.xinshi.pigeon.server.distributedserver.idserver.IdServer;
import net.xinshi.pigeon.server.distributedserver.idserver.NettyIdServerHandler;
import net.xinshi.pigeon.server.distributedserver.listserver.ListServer;
import net.xinshi.pigeon.server.distributedserver.listserver.NettyListServerHandler;
import net.xinshi.pigeon.server.distributedserver.lockserver.NettyLockServerHandler;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.TimeTools;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午2:24
 * To change this template use File | Settings | File Templates.
 */

public class PigeonServer implements IServerHandler {

    static Logger logger = Logger.getLogger(PigeonServer.class.getName());
    static ThreadPoolExecutor executor;
    static ArrayBlockingQueue<Runnable> workingQue;
    static AtomicInteger threadCount;
    public static ServerController controller = null;
    public static String controlFile = null;
    public static int nFixedThread = 100;
    public static String httpPort = ServerConstants.defaultPort;

    public static AtomicLong getOperationsCount() {
        return operationsCount;
    }

    public static void setOperationsCount(AtomicLong operationsCount) {
        PigeonServer.operationsCount = operationsCount;
    }

    static AtomicLong operationsCount;

    public static AtomicInteger getThreadCount() {
        return threadCount;
    }

    public static void setThreadCount(AtomicInteger threadCount) {
        PigeonServer.threadCount = threadCount;
    }

    public static void main(String[] args) throws Exception {
        try {
            String configFile = ServerConstants.defaultConfigFile;
            String port = ServerConstants.defaultPort;
            String nettyPort = ServerConstants.defaultNettyPort;
            if (args.length > 0) {
                configFile = args[0];
                if (configFile.compareTo("command") == 0 && args.length == 4) {
                    ServerCommand sc = new ServerCommand(args[1], args[2], args[3]);
                    sc.invoke_cmd();
                    System.exit(0);
                    return;
                }
            }

            if (args.length > 1) {
                nettyPort = args[1];
            }

            if (args.length > 2) {
                nFixedThread = Integer.valueOf(args[2]);
            }

            logger.info("pigeon version : " + Constants.version);
            int listenPort = Integer.parseInt(port);
            httpPort = port;
            logger.info("listening:" + listenPort);
            logger.info("listening netty port: " + nettyPort);
            logger.info("using config file:" + configFile);
            logger.info("newFixedThread : " + nFixedThread);
            startServers(Integer.parseInt(nettyPort), configFile);
            System.out.println(TimeTools.getNowTimeString() + " PigeonServer Started. version = " + Constants.version);
        } catch (Exception ex) {
            ex.printStackTrace();
            while (true) {
                try {
                    Thread.sleep(3000);
                    System.exit(0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void startNettyServers(int nettyPort,String configfile) throws Exception {
        controller = new ServerController(configfile);
        controller.init();
        controller.start();
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        final ExecutionHandler eh = new ExecutionHandler(
                new ThreadPoolExecutor(5,500,20L,TimeUnit.SECONDS, new SynchronousQueue<Runnable>()));
        final ServerHandler sh = new ServerHandler(new PigeonServer());
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline cpl = pipeline(new PigeonDecoder(), eh, sh);
                return cpl;
            }
        });
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.bind(new InetSocketAddress(nettyPort));

        //register servers to zookeeper

    }

    public ChannelBuffer handler(Channel ch, byte[] buffer) {
        try {
            int sq = CommonTools.bytes2intJAVA(buffer, 4);
            short flag = CommonTools.bytes2shortJAVA(buffer, 8);
            int t = (flag >> 8) & 0xFF;
            int n = flag & 0xFF;
            ChannelBuffer out = null;
            String id = "";
            int f = t & 0xF0;
            if (f != 0) {
                t &= 0x0F;
            }


            logger.log(Level.FINEST, "type=" + t + ", n=" + n);

            if (controller == null && t != net.xinshi.pigeon.netty.common.Constants.CONTROL_TYPE) {
                System.out.println("starting ... cancel handler ... ");
                return null;
            }
            if(t == net.xinshi.pigeon.netty.common.Constants.FILE_TYPE){
                id = "/file" + n;
                Object obj = controller.getServers().get(id);
                if(obj instanceof FileServer){
                    InputStream is = new ByteArrayInputStream(buffer, ServerConstants.PACKET_PREFIX_LENGTH, buffer.length - ServerConstants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = ((FileServer) obj).handle(is,f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }
            }
            else if (t == net.xinshi.pigeon.netty.common.Constants.FLEXOBJECT_TYPE) {
                id = "/flexobject" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof FlexObjectServer) {
                    InputStream is = new ByteArrayInputStream(buffer, ServerConstants.PACKET_PREFIX_LENGTH, buffer.length - ServerConstants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = NettyFlexObjectServerHandler.handle((FlexObjectServer) obj, is, f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }
                else{
                    logger.log(Level.WARNING, "flexObject server not existed,id=" + id);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.LIST_TYPE) {
                id = "/list" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof ListServer) {
                    InputStream is = new ByteArrayInputStream(buffer, ServerConstants.PACKET_PREFIX_LENGTH, buffer.length - ServerConstants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = NettyListServerHandler.handle((ListServer) obj, is, f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }
                else{
                    logger.log(Level.WARNING, "list server not existed,id=" + id);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.ATOM_TYPE) {
                id = "/atom" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof AtomServer) {
                    InputStream is = new ByteArrayInputStream(buffer, ServerConstants.PACKET_PREFIX_LENGTH, buffer.length - ServerConstants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = NettyAtomServerHandler.handle((AtomServer) obj, is, f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }
                else{
                    logger.log(Level.WARNING, "atom server not existed,id=" + id);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.ID_TYPE) {
                id = "/idserver" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof IdServer) {
                    InputStream is = new ByteArrayInputStream(buffer, ServerConstants.PACKET_PREFIX_LENGTH, buffer.length - ServerConstants.PACKET_PREFIX_LENGTH);
                    ByteArrayOutputStream os = NettyIdServerHandler.handle((IdServer) obj, is, f);
                    if (os == null) {
                        return null;
                    }
                    out = Tools.buildChannelBuffer(sq, flag, os);
                }else{
                    logger.log(Level.WARNING, "id server not existed,id=" + id);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.LOCK_TYPE) {
                id = "/lock" + n;
                Object obj = controller.getServers().get(id);
                if (obj instanceof NettyLockServerHandler) {
                    NettyLockServerHandler lockServer = (NettyLockServerHandler) obj;
                    out = lockServer.handler(ch, buffer);
                }
                else{
                    logger.log(Level.WARNING, "lock server not existed,id=" + id);
                }
            } else if (t == net.xinshi.pigeon.netty.common.Constants.CONTROL_TYPE) {
                long cmd = CommonTools.bytes2longJAVA(buffer, ServerConstants.PACKET_PREFIX_LENGTH);
                int state_word = net.xinshi.pigeon.netty.common.Constants.shift_state_word((int) cmd);
                if (!net.xinshi.pigeon.status.Constants.isAvailable(state_word)) {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    os.write("unknow command".getBytes());
                    out = Tools.buildChannelBuffer(sq, flag, os);
                    return out;
                }
                boolean rc = true;
                if (controller != null) {
                    rc = controller.set_servers_state_word(state_word);
                }
                String command = net.xinshi.pigeon.netty.common.Constants.get_state_string((int) cmd);
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                String info = "change server state word " + command + " return : " + rc;
                os.write(info.getBytes());
                out = Tools.buildChannelBuffer(sq, flag, os);
                logger.info(info);
                if (command.compareToIgnoreCase("stop") == 0) {
                    new Thread(new Runnable() {
                        public void run() {
                            while (true) {
                                try {
                                    Thread.sleep(3000);
                                    logger.warning("server stoped ... ");
                                    System.exit(0);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }).start();
                }
            }
            return out;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void channelConnected(Channel ch) throws Exception {
    }

    public void channelClosed(Channel ch) throws Exception {
        for (Object obj : controller.getServers().values()) {
            if (obj instanceof NettyLockServerHandler) {
                NettyLockServerHandler lockServer = (NettyLockServerHandler) obj;
                lockServer.channelClosed(ch);
            }
        }
    }

    public static void startServers(int nettyPort, String configfile) throws Exception {
        startNettyServers(nettyPort,configfile);
    }

    public static void stop() throws Exception {
        Collection<Object> servers = controller.getServers().values();
        for (Object o : servers) {
            if (o instanceof IServer) {
                ((IServer) o).stop();
            }
        }
    }



}

