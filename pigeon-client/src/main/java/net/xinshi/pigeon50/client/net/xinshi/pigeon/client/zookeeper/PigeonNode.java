package net.xinshi.pigeon50.client.net.xinshi.pigeon.client.zookeeper;

import net.xinshi.pigeon.netty.client.Client;
import org.json.JSONObject;

public class PigeonNode {
    String serviceHost;
    int servicePort;

    String type;
    String instanceName;
    int no;
    int typeCode;
    String nodeName;
    JSONObject extraData;
    String shardFullPath;
    String shardName;

    String ephemeralServerName;

    public String getEphemeralServerName() {
        return ephemeralServerName;
    }

    public void setEphemeralServerName(String ephemeralServerName) {
        this.ephemeralServerName = ephemeralServerName;
    }

    Client client;
    boolean isInited = false;


    public final static int TYPE_CODE_FLEXOBJECT = 0x1;
    public final static int TYPE_CODE_LIST = 0x2;
    public final static int TYPE_CODE_ATOM = 0x3;
    public final static int TYPE_CODE_IDSERVER = 0x4;
    public final static int TYPE_CODE_LOCK = 0x5;
    public final static int TYPE_CODE_FILE = 0x6;

    public static PigeonNode newInstance(String ephemeralServerName,JSONObject jsonObject){
        PigeonNode pnode = new PigeonNode();
        pnode.setServiceHost(jsonObject.optString("serviceHost"));
        pnode.setServicePort(jsonObject.optInt("servicePort"));
        pnode.setNodeName(jsonObject.optString("nodeName"));
        pnode.setEphemeralServerName(ephemeralServerName);
        pnode.setType(jsonObject.optString("type"));
        pnode.setInstanceName(jsonObject.optString("instanceName"));
        pnode.setShardFullPath(jsonObject.optString("shardFullPath"));
        int no =  Integer.valueOf(pnode.getInstanceName().substring(pnode.getInstanceName().length()-1)) & 0xFF;
        pnode.setNo(no);
        return pnode;
    }
    public static int getTypeCode(String type) {
        int t = 0;
        if (type.equals("flexobject")) {
            t = 0x1;
        } else if (type.equals("list")) {
            t = 0x2;
        } else if (type.equals("atom")) {
            t = 0x3;
        } else if (type.equals("idserver")) {
            t = 0x4;
        } else if (type.equals("lock")) {
            t = 0x5;
        } else if (type.equals("file")) {
            t = 0x6;
        }
        return t;
    }


    /*if (type.equals("flexobject")) {
        t = 0x1;
    } else if (type.equals("list")) {
        t = 0x2;
    } else if (type.equals("atom")) {
        t = 0x3;
    } else if (type.equals("idserver")) {
        t = 0x4;
    } else if (type.equals("lock")) {
        t = 0x5;
    }*/
    public String getServiceHost() {
        return serviceHost;
    }

    public void setServiceHost(String serviceHost) {
        this.serviceHost = serviceHost;
    }

    public int getServicePort() {
        return servicePort;
    }

    public void setServicePort(int servicePort) {
        this.servicePort = servicePort;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
        typeCode = getTypeCode(type);
    }

    public int getNo() {
        return no;
    }

    public void setNo(int no) {
        this.no = no;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }


    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void clean() {
        this.client.closeAll();
    }

    public int getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(int typeCode) {
        this.typeCode = typeCode;
    }

    public JSONObject getExtraData() {
        return extraData;
    }

    public void setExtraData(JSONObject extraData) {
        this.extraData = extraData;
    }

    public void init_client() {
        if (!isInited) {
            isInited = true;
            Client c = new Client(this.serviceHost, this.servicePort, 2);
            c.init();
            this.client = c;
        }

    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public String getShardFullPath() {
        return shardFullPath;
    }

    public void setShardFullPath(String shardFullPath) {
        this.shardFullPath = shardFullPath;
    }
}
