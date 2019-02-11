package net.xinshi.pigeon50.client.net.xinshi.pigeon.client.zookeeper;

import java.util.List;

public class Shard {
    public static int SHARD_OK;
    public static int SHARD_SWITCHING;
    public static int SHARD_HEAD_CATCHING;

    int minHash;
    int maxHash;
    String fullPath;
    String name;
    int state;
    List<PigeonNode> servers;

    public int getMinHash() {
        return minHash;
    }

    public void setMinHash(int minHash) {
        this.minHash = minHash;
    }

    public int getMaxHash() {
        return maxHash;
    }

    public void setMaxHash(int maxHash) {
        this.maxHash = maxHash;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<PigeonNode> getServers() {
        return servers;
    }

    public void setServers(List<PigeonNode> servers) {
        this.servers = servers;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getFullPath() {
        return fullPath;
    }

    public void setFullPath(String fullPath) {
        this.fullPath = fullPath;
    }

    public PigeonNode getMaster(){
        if(servers.size()>=1){
            return servers.get(0);
        }
        else{
            return null;
        }

    }
}
