package net.xinshi.pigeon.filesystem;

import org.json.JSONObject;

public class FileServerRec {
    String ip;
    int port;
    String internalUrl;
    String externalUrl;
    String nodeName;
    String shardName;
    String shardExternalUrl;
    String shardInternalUrl;
    String shardFullPath;


    String instanceName;

    public FileServerRec(){

    }

    public void setData(JSONObject json){
        this.ip = json.optString("ip");
        this.port = json.optInt("port");
        this.internalUrl = json.optString("internalUrl");
        this.externalUrl = json.optString("externalUrl");
        this.shardExternalUrl = json.optString("shardExternalUrl");
        this.shardInternalUrl = json.optString("shardInternalUrl");
        this.nodeName = json.optString("nodeName");
        this.shardName = json.optString("shardName");
        this.instanceName = json.optString("instanceName");
        this.shardFullPath = json.optString("shardFullPath");
    }

    public FileServerRec(JSONObject json){
        this.ip = json.optString("ip");
        this.port = json.optInt("port");
        this.internalUrl = json.optString("internalUrl");
        this.externalUrl = json.optString("externalUrl");
        this.shardExternalUrl = json.optString("shardExternalUrl");
        this.shardInternalUrl = json.optString("shardInternalUrl");
        this.nodeName = json.optString("nodeName");
        this.shardName = json.optString("shardName");
        this.instanceName = json.optString("instanceName");
        this.shardFullPath = json.optString("shardFullPath");
    }

    public String getShardExternalUrl() {
        return shardExternalUrl;
    }

    public void setShardExternalUrl(String shardExternalUrl) {
        this.shardExternalUrl = shardExternalUrl;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getInternalUrl() {
        return internalUrl;
    }

    public void setInternalUrl(String internalUrl) {
        this.internalUrl = internalUrl;
    }

    public String getExternalUrl() {
        return externalUrl;
    }

    public void setExternalUrl(String externalUrl) {
        this.externalUrl = externalUrl;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getShardInternalUrl() {
        return shardInternalUrl;
    }

    public void setShardInternalUrl(String shardInternalUrl) {
        this.shardInternalUrl = shardInternalUrl;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getShardFullPath() {
        return shardFullPath;
    }

    public void setShardFullPath(String shardFullPath) {
        this.shardFullPath = shardFullPath;
    }
}
