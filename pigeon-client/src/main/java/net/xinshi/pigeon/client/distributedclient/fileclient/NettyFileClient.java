package net.xinshi.pigeon.client.distributedclient.fileclient;

import net.xinshi.pigeon.client.net.xinshi.pigeon.client.zookeeper.NodesDispatcher;
import net.xinshi.pigeon.client.net.xinshi.pigeon.client.zookeeper.PigeonNode;
import net.xinshi.pigeon.filesystem.FileID;
import net.xinshi.pigeon.filesystem.FileServerRec;
import net.xinshi.pigeon.filesystem.IFileSystem;

import net.xinshi.pigeon.util.CommonTools;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.commons.lang.StringUtils;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClientBuilder;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static net.xinshi.pigeon.client.util.ZkUtil.getData;

public class NettyFileClient implements IFileSystem {

    Map<String, List<FileServerRec>> shards = new HashMap();
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);

    /***fileds***/
    ZooKeeperClient ztc;
    String zkConnectString;
    String clusterPath;
    NodesDispatcher nodesDispatcher;
    /***fields***/

    public NettyFileClient(NodesDispatcher nodesDispatcher,String zkConnectString, String clusterPath){
        this.zkConnectString = zkConnectString;
        this.clusterPath = clusterPath;
        this.nodesDispatcher = nodesDispatcher;
    }

    public void init() throws Exception {
        ztc = ZooKeeperClientBuilder.newBuilder()
                .sessionTimeoutMs(40000)
                .retryThreadCount(2)
                .requestRateLimit(200)
                .zkServers(zkConnectString)
                .zkAclId("pigeon40FileClient")
                .retryPolicy(new BoundExponentialBackoffRetryPolicy(500, 1500, 2))
                .build();
        downloadFileServerRecs(ztc.get(),clusterPath);
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    downloadFileServerRecs(ztc.get(),clusterPath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },30,TimeUnit.SECONDS);
    }


    //获得根据/年/月/日，分隔的路径
    String getDateFilePath() throws Exception {
        /*SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
        String today = format.format(new Date());
        String fileId = "/" + today;*/
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        String fileId = "/" + year + "/" + month + "/" + day;
        return fileId;
    }


    String getExtension(File f) {
        String fullPath = f.getName();
        int rpos = fullPath.lastIndexOf(".");
        if (rpos < 0) {
            return "";
        } else {
            return fullPath.substring(rpos);
        }

    }

    String getExtension(String fullPath) {
        int rpos = fullPath.lastIndexOf(".");
        if (rpos < 0) {
            return "";
        } else {
            return fullPath.substring(rpos);
        }
    }

    String getKeyFromFileId(String fileId){
        int pos = fileId.lastIndexOf("/");
        String fileName = fileId.substring(pos);
        int dotPos = fileName.lastIndexOf(".");
        if(dotPos<0){
            return fileName;
        }
        String key = fileName.substring(0,dotPos);
        return key;
    }


    FileServerRec getFileServerRec(String shardFullPath, String nodeName, String instanceName){
        List<FileServerRec> svrRecs = shards.get(shardFullPath);
        for(FileServerRec rec : svrRecs){
            if(rec.getNodeName().equals(nodeName) && rec.getInstanceName().contains(instanceName)){
                return rec;
            }
        }
        return svrRecs.get(0);
    }

    @Override
    public void delete(String fileid) throws Exception {
        String key =getKeyFromFileId(fileid);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out,"delete");
        CommonTools.writeString(out,fileid);
        InputStream is = nodesDispatcher.commit(PigeonNode.TYPE_CODE_FILE,key,out);
        String state = CommonTools.readString(is);
        if(StringUtils.equals("ok",state)){
            return;
        }
        throw new Exception(state);

    }

    @Override
    public String getUrl(String fileid) throws Exception {
        FileID fileRec = FileID.parse(fileid);
        //根据nodeName 和shardName获得url
        FileServerRec fileServerRec = getFileServerRec(fileRec.getShardName(),fileRec.getNodeName(), fileRec.getInstanceName());
        return fileServerRec.getShardExternalUrl() + fileRec.getPath();
    }

    @Override
    public String getInternalUrl(String fileId) throws Exception {
        FileID fileRec = FileID.parse(fileId);
        //根据nodeName 和shardName获得url
        FileServerRec fileServerRec = getFileServerRec(fileRec.getShardName(),fileRec.getNodeName(), fileRec.getInstanceName());
        return fileServerRec.getInternalUrl() + fileRec.getPath();
    }

    @Override
    public OutputStream openOutputSystem(String fileId) throws Exception {
        return null;
    }

    @Override
    public String checkExists(File f) throws Exception, IOException {
        return null;
    }



    @Override
    public String addFile(File f, String name) throws Exception {
        String key = UUID.randomUUID().toString();
        PigeonNode pnode = nodesDispatcher.getPigeonNode(PigeonNode.TYPE_CODE_FILE,key);
        String svrPart = pnode.getShardName() + "#" + pnode.getNodeName() + "#" + pnode.getInstanceName().substring(1);
        String path = getDateFilePath() + "/" + key + getExtension(f);
        String fileId = svrPart + "@" + path;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out,fileId);
        CommonTools.writeLong(out,f.length());
        FileInputStream fis = new FileInputStream(f);
        byte[] buf = new byte[8192];
        int n;
        while ((n = fis.read(buf)) != -1) {
            CommonTools.writeBytes(out, buf, 0, n);
//            out.write(buf,0,n);
        }
        fis.close();
        InputStream ris = nodesDispatcher.commitToNode(PigeonNode.TYPE_CODE_FILE,pnode,out);
        String state = CommonTools.readString(ris);
        if(StringUtils.equals(state,"ok")){
            return fileId;
        }
        else{
            throw new Exception(state);
        }
    }

    @Override
    public String addBytes(byte[] bytes, String name) throws Exception {
        String key = UUID.randomUUID().toString();
        PigeonNode pnode = nodesDispatcher.getPigeonNode(PigeonNode.TYPE_CODE_FILE,key);
        String svrPart = pnode.getShardName() + "#" + pnode.getNodeName() + "#" + pnode.getInstanceName().substring(1);
        String path = getDateFilePath() + "/" + key + getExtension(name);
        String fileId = svrPart + "@" + path;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommonTools.writeString(out,"add");
        CommonTools.writeString(out,fileId);
        CommonTools.writeLong(out,bytes.length);
        CommonTools.writeBytes(out, bytes, 0, bytes.length);
        InputStream ris = nodesDispatcher.commitToNode(PigeonNode.TYPE_CODE_FILE,pnode,out);
        String state = CommonTools.readString(ris);
        if(StringUtils.equals(state,"ok")){
            return fileId;
        }
        else{
            throw new Exception(state);
        }
    }

    @Override
    public String getRelatedUrl(String fileId, String spec) {
        try {
            String url = getUrl(fileId);
            int index = url.lastIndexOf(".");
            return url.substring(0, index) + "_" + spec + url.substring(index);
        } catch (Exception e) {
//            System.out.println("fileId = " + fileId + ", spec = " + spec + ", getRelatedUrl exception");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void genRelatedFile(String fileId, String spec) throws Exception {

    }

    @Override
    public List<String> getUrls(String fileId) throws Exception {
        return null;
    }

    private void downloadFileServerRecs(ZooKeeper zk,String fileClusterPath) throws Exception {
        Map<String, List<FileServerRec>> newShards = new HashMap();

        List<String> shards = zk.getChildren(fileClusterPath, false);
        for(String shard : shards){
            List<FileServerRec> serverRecs = new ArrayList<FileServerRec>();
            String shardFullPath = fileClusterPath + "/" + shard;
            newShards.put(shard,serverRecs);

            List<String> svrs = zk.getChildren(shardFullPath, false);
            for(String svr : svrs){
                String svrPath = shardFullPath + "/"  + svr;
                JSONObject svrData = getData(zk,svrPath);
                FileServerRec fileServerRec = new FileServerRec(svrData);
                fileServerRec.setShardName(shard);
                serverRecs.add(fileServerRec);
            }
        }

        this.shards = newShards;
    }


}
