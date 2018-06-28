package net.xinshi.pigeon.filesystem;

public class FileID {
    String path;
    String shardName;
    String instanceName;
    String nodeName;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
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

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public static FileID parse(String fileId){
        String[] pair = fileId.split("@");
        FileID fid = new FileID();
        fid.path = pair[1];
        String[] serverParts = pair[0].split("#");
        fid.shardName = serverParts[0];
        fid.nodeName = serverParts[1];
        fid.instanceName = serverParts[2];

        return fid;

    }
}
