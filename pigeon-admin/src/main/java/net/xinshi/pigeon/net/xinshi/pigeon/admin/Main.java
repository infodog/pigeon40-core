package net.xinshi.pigeon.net.xinshi.pigeon.admin;

import org.apache.commons.lang.StringUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.acl.Acl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static org.apache.zookeeper.CreateMode.PERSISTENT;

public class Main {
    static String g_zooConnectString;
    public static void main(String[] args) throws IOException, JSONException, KeeperException, InterruptedException, URISyntaxException {
        int max_int = Integer.MAX_VALUE;
        String zooConnectString = args[0];
        g_zooConnectString = zooConnectString;
        ZooKeeper zk = new ZooKeeper(zooConnectString,8000,null);

        //读取配置文件
        String configFile = args[1];
        File fconf = new File(configFile);
        FileInputStream fis = new FileInputStream(configFile);
        byte[] bytes = new byte[(int) fconf.length()];
        fis.read(bytes);
        String configString = new String(bytes,"utf-8");

        String podName = "pigeon50Default";
        if(args.length>2){
            podName = args[2];
        }

        configString = configString.replaceAll("%podname%",podName);
        JSONObject jconfig = new JSONObject(configString);
        System.out.println("connectString=" + zooConnectString + ",podName=" + podName + ", configString=" + configString);

        try {
            zk.create("/pigeon50", "pigeon50 root".getBytes("utf-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        }
        catch(Exception e){
            //do nothing;
        }
        //检查jconfig是否和zookeeper商现有的数据冲突
        //如果当前的pod已经存在，那么就退出, 如果当前pod不存在，那么就建立当前的pod


        JSONArray jpods = jconfig.getJSONArray("pods");

        boolean exists = false;
        for(int i=0; i < jpods.length(); i++){
            JSONObject jpod = jpods.getJSONObject(i);
//            String podName = jpod.getString("name");
            //检查podName是否已经存在
            Stat stat = zk.exists("/pigeon50/" + podName,null);
            if(stat!=null){
                exists = true;
                System.out.println(podName + " alread exists.");
            }
            else{
                System.out.println(podName + " is ok.");
            }
        }
        for(int i=0; i < jpods.length(); i++){
            JSONObject jpod = jpods.getJSONObject(i);
            createPod(zk,jpod);
        }
        System.out.println("pod created！");
        Thread.sleep(1000);
        System.exit(0);
    }



    private static void createPod(ZooKeeper zk, JSONObject jpod) throws JSONException, KeeperException, InterruptedException, IOException, URISyntaxException {
        String podName = jpod.optString("name");
        String podDesc = jpod.optString("desc");
        JSONObject podData = new JSONObject();
        podData.put("desc", podDesc);
        String podPath = "/pigeon50/" + podName;
        List<ACL> acl = new ArrayList();
        try {
            zk.create(podPath, podData.toString().getBytes("utf-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        JSONObject object_cluster = jpod.optJSONObject("object_cluster");
        JSONObject list_cluster = jpod.optJSONObject("list_cluster");
        JSONObject id_cluster = jpod.optJSONObject("id_cluster");
        JSONObject atom_cluster = jpod.optJSONObject("atom_cluster");
        JSONObject locks_cluster = jpod.optJSONObject("locks_cluster");
        JSONObject file_cluster = jpod.optJSONObject("file_cluster");

//        String namespaceUri = jpod.getString("namespace");

        createCluster(zk, podPath, object_cluster, "object_cluster");
        createCluster(zk, podPath, list_cluster, "list_cluster");
        createCluster(zk, podPath, id_cluster, "id_cluster");
        createCluster(zk, podPath, atom_cluster, "atom_cluster");
        createCluster(zk, podPath, locks_cluster, "locks_cluster");
        createCluster(zk, podPath, file_cluster, "file_cluster");
        System.out.println("create pod succeeded.");
    }

    private static void createLogTopic(String logName, AdminClient kafkaAdminClient){
        //kafka topic
        NewTopic topic = new NewTopic(logName,1, (short) 1);
        kafkaAdminClient.createTopics(Arrays.<NewTopic>asList(topic));
    }

    private static void createCluster(ZooKeeper zk,String parentPath, JSONObject cluster,String type) throws JSONException, IOException, KeeperException, InterruptedException {
        JSONObject data = new JSONObject();
        data.put("name",cluster.optString("name"));
        data.put("type", type);
        String clusterPath = parentPath + "/" + type;
        java.util.Properties properties = new java.util.Properties();

        properties.setProperty("bootstrap.servers",g_zooConnectString);
        properties.setProperty("client.id","pigeon50.admin");
        AdminClient kafkaAdminClient = AdminClient.create(properties);

        try {

            zk.create(clusterPath, data.toString().getBytes("utf-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
            System.out.println("create node " + clusterPath);
            JSONArray shards = cluster.optJSONArray("shards");
            if (shards != null) {
                for (int i = 0; i < shards.length(); i++) {
                    JSONObject jshard = shards.optJSONObject(i);
                    JSONObject shardData = jshard;
                    String shardPath = clusterPath + "/shard" + i;
                    zk.create(shardPath, shardData.toString().getBytes("utf-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
                    String logName = shardData.optString("logName");
                    if (StringUtils.isNotBlank(logName)) {
                        createLogTopic(logName,kafkaAdminClient);
                    }

                }
            }
            System.out.println("create node " + clusterPath + " succeeded!");
        }
        catch(Exception e){
            System.out.println("create cluster failed:" + clusterPath);
        }

    }
}
