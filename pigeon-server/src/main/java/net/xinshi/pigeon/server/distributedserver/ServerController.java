package net.xinshi.pigeon.server.distributedserver;



import net.xinshi.pigeon.server.distributedserver.atomserver.AtomServer;
import net.xinshi.pigeon.server.distributedserver.atomserver.AtomServerFactory;

import net.xinshi.pigeon.server.distributedserver.fileserver.FileServerFactory;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.FlexObjectServer;
import net.xinshi.pigeon.server.distributedserver.flexobjectserver.FlexObjectServerFactory;
import net.xinshi.pigeon.server.distributedserver.idserver.IdServer;
import net.xinshi.pigeon.server.distributedserver.idserver.IdServerFactory;
import net.xinshi.pigeon.server.distributedserver.listserver.ListServer;
import net.xinshi.pigeon.server.distributedserver.listserver.ListServerFactory;
import net.xinshi.pigeon.server.distributedserver.lockserver.NettyLockServerHandler;
import net.xinshi.pigeon.server.distributedserver.util.Tools;
import net.xinshi.pigeon.server.distributedserver.writeaheadlog.ILogManager;
import net.xinshi.pigeon.server.distributedserver.writeaheadlog.KafkaLogManager;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 下午2:32
 * To change this template use File | Settings | File Templates.
 */

public class ServerController {

    List<ServerConfig> listConfigs;
    Map<String, Object> servers;
    String zkConnectString = null;
    String kafkaServers = null;

    Logger log = Logger.getLogger(ServerController.class.getName());
    String configfile = null;
    Logger logger = Logger.getLogger(ServerController.class.getName());

    public ServerController(String configfile) {
        this.configfile = configfile;
        servers = new Hashtable<String, Object>();
    }


    synchronized public IServer start(ZooKeeper zk, ServerConfig sc) throws Exception {
        String instanceName = sc.getInstanceName();
        String nodeName = sc.getNodeName();
        if (StringUtils.isBlank(nodeName)) {
            log.log(Level.SEVERE, "serverName can not be null.");
            return null;
        }
        if (StringUtils.isBlank(instanceName)) {
            log.log(Level.SEVERE, "instanceName can not be null.");
            return null;
        }
        if (servers.containsKey(instanceName)) {
            throw new Exception(instanceName + ": Server already exists.");
        }
        String type = sc.getType();
        IServer server = null;
        if ("flexobject".equals(type)) {
            logger.info("going to start flexobject server of instance name = " + instanceName + " ...");
            FlexObjectServerFactory factory = new FlexObjectServerFactory(sc);
            ILogManager logManager = buildLogManager(zk,sc);
            logManager.setAsync(false);
            logger.info("after buildLogManager. instance name = " + instanceName + " ...");
            factory.setLogManager(logManager);
            factory.setZk(zk);
            server = factory.createFlexObjectServer();
            logger.info("after createFlexObjectServer. instance name = " + instanceName + " ...");
            server.start();
            logger.info("after startFlexObjectServer. instance name = " + instanceName + " ...");
            servers.put(instanceName, server);
            logger.info("started flexobject server of instance name = " + instanceName + " ...");
        } else if ("list".equals(type)) {
            logger.info("going to start list server of instance name = " + instanceName + " ...");
            ListServerFactory factory = new ListServerFactory(sc);
            logger.info("new ListServerFactory, instance name = " + instanceName + " ...");
            ILogManager logManager = buildLogManager(zk,sc);
            logger.info("buildLogManager, instance name = " + instanceName + " ...");
            factory.setLogManager(logManager);
            factory.setZk(zk);
            logger.info("before createListServer, instance name = " + instanceName + " ...");
            server = factory.createListServer();
            logger.info("after createListServer, instance name = " + instanceName + " ...");
            servers.put(instanceName, server);
            server.start();
            logger.info("started list server of instance name = " + instanceName + " ...");
        } else if ("atom".equals(type)) {
            logger.info("going to start atom server of instance name = " + instanceName + " ...");
            AtomServerFactory factory = new AtomServerFactory(sc);
            ILogManager logManager = buildLogManager(zk,sc);
            factory.setLogManager(logManager);
            factory.setZk(zk);
            server = factory.createAtomServer();
            server.start();
            servers.put(instanceName, server);
            logger.info("started atom server of instance name = " + instanceName + " ...");
        } else if ("lock".equals(type)) {
            logger.info("going to start lock  server of instance name = " + instanceName + " ...");
            NettyLockServerHandler lockServer = new NettyLockServerHandler(sc);
            servers.put(instanceName, lockServer);
            server = null;
            lockServer.init();
            BaseServer.registerServerToZookeeper(zk,sc);
            logger.info("started lock server of instance name = " + instanceName + " ...");

        } else if ("idserver".equals(type)) {
            logger.info("started id server of instance name = " + instanceName + " ...");
            IdServerFactory idServerFactory = new IdServerFactory(sc);
            ILogManager logManager = buildLogManager(zk,sc);
            idServerFactory.setLogManager(logManager);
            idServerFactory.setZk(zk);
            server = idServerFactory.createIdServer();
            server.start();
            servers.put(instanceName, server);
            logger.info("started id server of instance name = " + instanceName + " ...");
        }
        else if ("fileserver".equals(type)) {
            logger.info("going to start file server of instance name = " + instanceName + " ...");
            FileServerFactory fileServerFactory = new FileServerFactory(sc);
            ILogManager logManager = buildLogManager(zk,sc);
            fileServerFactory.setLogManager(logManager);
            fileServerFactory.setZk(zk);
            server = fileServerFactory.createFileServer();
            server.start();
            servers.put(instanceName, server);
            logger.info("started file server of instance name = " + instanceName + " ...");
        }
        return server;
    }

    public static JSONObject getData(ZooKeeper zk , String path) throws KeeperException, InterruptedException, UnsupportedEncodingException, JSONException {
        Stat stat = new Stat();
        byte[] data = zk.getData(path,false,stat);
        String s = new String(data,"utf-8");
        JSONObject jsonObject = new JSONObject(s);
        return jsonObject;
    }


    ILogManager buildLogManager(ZooKeeper zk, ServerConfig sc) throws InterruptedException, JSONException, KeeperException, UnsupportedEncodingException {
        JSONObject shardData = getData(zk,sc.shardFullPath);
        String logName = shardData.getString("logName");
        KafkaLogManager logManager = new KafkaLogManager();
        logManager.setBootstrapServers(kafkaServers);
        logManager.setTopic(logName);
        logManager.setGroupId(sc.getNodeName());
        logManager.init();
        return logManager;
    }


    public void startServers() throws Exception {
        ZooKeeper zk = new ZooKeeper(zkConnectString,300000,null);
        for (ServerConfig sc : listConfigs) {
            try {
                start(zk,sc);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    List<ServerConfig> createServerConfigs(String s,int nettyPort) throws Exception {
        s = StringUtils.trim(s);
        JSONObject jo = new JSONObject(s);
        zkConnectString = jo.optString("zk");
        kafkaServers = jo.optString("kafkaSrevers");

        JSONArray jconfigs = jo.getJSONArray("pigeons");
        List<ServerConfig> listSC = new ArrayList<ServerConfig>();
        for (int j = 0; j < jconfigs.length(); j++) {
            JSONObject jconfig = jconfigs.getJSONObject(j);
            ServerConfig sc = ServerConfig.JSONObject2ServerConfig(jconfig);
            sc.servicePort = nettyPort;
            listSC.add(sc);
        }
        return listSC;
    }

    public void init(int nettyPort) throws Exception {
        if (configfile != null) {
            File f = new File(configfile);
            FileInputStream is = new FileInputStream(f);
            byte[] b = new byte[(int) f.length()];
            is.read(b);
            is.close();
            String s = new String(b, "UTF-8");
            s = Tools.expandEnvOrSystemProperties(s);
            listConfigs = createServerConfigs(s,nettyPort);
            return;
        }
        throw new Exception("ServerController init error ...... ");
    }



    public void start() throws Exception {
        startServers();

    }

    public Map<String, Object> getServers() {
        return servers;
    }

    public boolean set_servers_state_word(int state_word) {
        try {
            for (Object obj : servers.values()) {
                if (obj instanceof FlexObjectServer) {
                    ((FlexObjectServer) obj).set_state_word(state_word);
                } else if (obj instanceof ListServer) {
                    ((ListServer) obj).getFactory().set_state_word(state_word);
                } else if (obj instanceof AtomServer) {
                    ((AtomServer) obj).getAtom().set_state_word(state_word);
                } else if (obj instanceof IdServer) {
                    ((IdServer) obj).getIdgenerator().set_state_word(state_word);
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

}

