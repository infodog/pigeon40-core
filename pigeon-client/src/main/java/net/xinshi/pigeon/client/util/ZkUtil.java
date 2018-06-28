package net.xinshi.pigeon.client.util;

import net.xinshi.pigeon.client.net.xinshi.pigeon.client.zookeeper.PigeonNode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;

public class ZkUtil {
    public static JSONObject getData(ZooKeeper zk , String path) throws KeeperException, InterruptedException, UnsupportedEncodingException, JSONException {
        Stat stat = new Stat();
        byte[] data = zk.getData(path,false,stat);
        String s = new String(data,"utf-8");
        JSONObject jsonObject = new JSONObject(s);
        return jsonObject;
    }


    public static PigeonNode getPigeonNode(ZooKeeper zk, String shardPath,String svrName) throws InterruptedException, JSONException, KeeperException, UnsupportedEncodingException {
        String fullPath = shardPath + "/" + svrName;
        JSONObject jsonObject = getData(zk,fullPath);
        return PigeonNode.newInstance(svrName,jsonObject);
    }
}

