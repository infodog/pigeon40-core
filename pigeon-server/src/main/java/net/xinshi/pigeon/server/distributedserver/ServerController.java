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
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.commons.lang.StringUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClientBuilder;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
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
    Logger log = Logger.getLogger(ServerController.class.getName());
    String configfile = null;
    Logger logger = Logger.getLogger(ServerController.class.getName());

    public ServerController(String configfile) {
        this.configfile = configfile;
        servers = new Hashtable<String, Object>();
    }

    Namespace buildNamespace() throws URISyntaxException, IOException {
        // DistributedLog Configuration
        DistributedLogConfiguration conf = new DistributedLogConfiguration().setLockTimeout(10);
        // Namespace URI
        URI uri = new URI("distributedlog://"+zkConnectString + "/pigeon40namespace"); // create the namespace uri
        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        newConf.setCreateStreamIfNotExists(false);
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(newConf).uri(uri).build();
        return namespace;
    }

    synchronized public IServer start(ZooKeeperClient ztc,Namespace namespace, ServerConfig sc) throws Exception {
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
            DistributedLogManager dlm = buildDLM(ztc,namespace,sc);
            FlexObjectServerFactory factory = new FlexObjectServerFactory(sc);
            factory.setDlm(dlm);
            factory.setZtc(ztc);
            server = factory.createFlexObjectServer();
            server.start();
            servers.put(instanceName, server);
            System.out.println("started flexobject server of instance name = " + instanceName + " ...");
        } else if ("list".equals(type)) {
            ListServerFactory factory = new ListServerFactory(sc);
            DistributedLogManager dlm = buildDLM(ztc,namespace,sc);
            factory.setDlm(dlm);
            factory.setZtc(ztc);
            server = factory.createListServer();
            servers.put(instanceName, server);
            server.start();
            System.out.println("started list server of instance name = " + instanceName + " ...");
        } else if ("atom".equals(type)) {
            AtomServerFactory factory = new AtomServerFactory(sc);
            DistributedLogManager dlm = buildDLM(ztc,namespace,sc);
            factory.setDlm(dlm);
            factory.setZtc(ztc);
            server = factory.createAtomServer();
            server.start();
            servers.put(instanceName, server);
            System.out.println("started atom of instance name = " + instanceName + " ...");
        } else if ("lock".equals(type)) {
            NettyLockServerHandler lockServer = new NettyLockServerHandler(sc);
            servers.put(instanceName, lockServer);
            server = null;
            lockServer.init();
            BaseServer.registerServerToZookeeper(ztc.get(),sc);
            ztc.registerExpirationHandler(new ZooKeeperClient.ZooKeeperSessionExpireNotifier() {
                @Override
                public void notifySessionExpired() {
                    try {
                        BaseServer.registerServerToZookeeper(ztc.get(),sc);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
                        e.printStackTrace();
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
            });
            System.out.println("started lock of instance name = " + instanceName + " ...");
        } else if ("idserver".equals(type)) {
            IdServerFactory idServerFactory = new IdServerFactory(sc);
            DistributedLogManager dlm = buildDLM(ztc,namespace,sc);
            idServerFactory.setDlm(dlm);
            idServerFactory.setZtc(ztc);
            server = idServerFactory.createIdServer();
            server.start();
            servers.put(instanceName, server);
            System.out.println("started id of instance name = " + instanceName + " ...");
        }
        else if ("fileserver".equals(type)) {
            FileServerFactory fileServerFactory = new FileServerFactory(sc);
            DistributedLogManager dlm = buildDLM(ztc,namespace,sc);
            fileServerFactory.setDlm(dlm);
            fileServerFactory.setZtc(ztc);
            server = fileServerFactory.createFileServer();
            server.start();
            servers.put(instanceName, server);
            System.out.println("started fileServer of instance name = " + instanceName + " ...");
        }
        return server;
    }

    public static JSONObject getData(ZooKeeperClient zk , String path) throws KeeperException, InterruptedException, UnsupportedEncodingException, JSONException, ZooKeeperClient.ZooKeeperConnectionException {
        Stat stat = new Stat();
        byte[] data = zk.get().getData(path,false,stat);
        String s = new String(data,"utf-8");
        JSONObject jsonObject = new JSONObject(s);
        return jsonObject;
    }


    DistributedLogManager buildDLM(ZooKeeperClient zk,Namespace namespace,ServerConfig sc) throws InterruptedException, JSONException, KeeperException, IOException {
        //get shared data
        JSONObject shardData = getData(zk,sc.shardFullPath);
        String logName = shardData.getString("logName");
        DistributedLogManager dlm = namespace.openLog(logName);
        return dlm;

    }

    public void startServers() throws Exception {

        ZooKeeperClient ztc = ZooKeeperClientBuilder.newBuilder()
                .sessionTimeoutMs(40000)
                .retryThreadCount(2)
                .requestRateLimit(200)
                .zkServers(zkConnectString)
                .zkAclId("pigeon40")
                .retryPolicy(new BoundExponentialBackoffRetryPolicy(500, 1500, 2))
                .build();
        Namespace namespace = buildNamespace();

        for (ServerConfig sc : listConfigs) {
            try {
                start(ztc,namespace,sc);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    List<ServerConfig> createServerConfigs(String s) throws Exception {
        s = StringUtils.trim(s);
        JSONObject jo = new JSONObject(s);
        zkConnectString = jo.optString("zk");
        JSONArray jconfigs = jo.getJSONArray("pigeons");
        List<ServerConfig> listSC = new ArrayList<ServerConfig>();
        for (int j = 0; j < jconfigs.length(); j++) {
            JSONObject jconfig = jconfigs.getJSONObject(j);
            ServerConfig sc = ServerConfig.JSONObject2ServerConfig(jconfig);
            listSC.add(sc);
        }
        return listSC;
    }

    public void init() throws Exception {
        if (configfile != null) {
            File f = new File(configfile);
            FileInputStream is = new FileInputStream(f);
            byte[] b = new byte[(int) f.length()];
            is.read(b);
            is.close();
            String s = new String(b, "UTF-8");
            logger.log(Level.INFO,s);

            listConfigs = createServerConfigs(s);
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

