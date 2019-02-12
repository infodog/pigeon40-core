package net.xinshi.pigeon.server.distributedserver.fileserver;

import net.xinshi.pigeon.filesystem.FileID;
import net.xinshi.pigeon.filesystem.FileServerRec;
import net.xinshi.pigeon.server.distributedserver.BaseServer;
import net.xinshi.pigeon.server.distributedserver.writeaheadlog.LogRecord;
import net.xinshi.pigeon.util.CommonTools;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.commons.io.FilenameUtils.getPath;


public class FileServer extends BaseServer {
    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    String baseDir;

    static final Logger logger = LoggerFactory.getLogger(FileServer.class);
    static HttpClientManager httpClientManager = new HttpClientManager();
    Thread downloadFileServerRecsThread = null;
    final private ExecutorService downloadFileService = new ThreadPoolExecutor(0, 20,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>()
            );
    List<FileServerRec> fileServerRecs;

    TreeSet<Long> finishedTxids;

    Object locker = new Object();

    private static final Logger LOG = LoggerFactory.getLogger(FileServer.class);

    public static JSONObject getData(ZooKeeper zk, String path) throws KeeperException, InterruptedException, UnsupportedEncodingException, JSONException {
        Stat stat = new Stat();
        byte[] data = zk.getData(path, false, stat);
        String s = new String(data, "utf-8");
        JSONObject jsonObject = new JSONObject(s);
        return jsonObject;
    }

    private void downloadFileServerRecs() throws  InterruptedException, KeeperException, UnsupportedEncodingException, JSONException {
        List<String> svrs = getZk().getChildren(getSc().getShardFullPath(), this);
        List<FileServerRec> fileServerRecs = new ArrayList<>();
        for (String svr : svrs) {
            String svrPath = getSc().getShardFullPath() + "/" + svr;
            JSONObject jsonObject = getData(getZk(), svrPath);

            FileServerRec fileServerRec = new FileServerRec(jsonObject);
            fileServerRecs.add(fileServerRec);
        }
        this.fileServerRecs = fileServerRecs;
    }

    String getInternalUrl(String nodeName, String instanceName){
        for(FileServerRec fileServerRec : this.fileServerRecs){
            if(StringUtils.equals(fileServerRec.getNodeName(),nodeName) && StringUtils.contains(fileServerRec.getInstanceName(),instanceName)){
                return fileServerRec.getInternalUrl();
            }
        }
        return null;
    }

    private synchronized void saveLocalVerion(long txid) throws IOException {
        File dir = new File(getSc().getLogDir());
        dir.mkdirs();
        File f = new File(getSc().getLogDir(), "ver.bin");
        FileOutputStream logos = new FileOutputStream(f);
        CommonTools.writeLong(logos,this.txid);
        logos.close();
    }

    private synchronized void setTxFinished(long txid) throws IOException {
        if(txid == this.txid+1){
            //更新
            this.txid = this.txid+1;
            SortedSet<Long> tailset = finishedTxids.tailSet(txid);
            for(Iterator<Long> it = tailset.iterator(); it.hasNext(); ){
                Long finishedTxid = it.next();
                if(finishedTxid==this.txid+1){
                    this.txid = this.txid+1;
                    it.remove();
                    continue;
                }
            }
            saveLocalVerion(txid);
        }
        else{
            finishedTxids.add(txid);
        }
    }

    private void downloadFile(String fileId,long txid) throws Exception {
            FileID fileRec = FileID.parse(fileId);
            String internalUrl = getInternalUrl(fileRec.getNodeName(),fileRec.getInstanceName());
            if(internalUrl==null){
                logger.warn("no internalUrl, the server maybe down. fileId=" + fileId );
                logger.error("download error:<" + fileId + "><" + txid + ">" );
                setTxFinished(txid);
                return;
            }
            internalUrl +=  fileRec.getPath();
        try {
            URL url = new URL(internalUrl);
            InputStream in = url.openStream();

            if(in == null){
                logger.warn("no internalUrl, the server maybe down. fileId=" + fileId );
                logger.error("download error:<" + fileId + "><" + txid + ">" );
                setTxFinished(txid);
                return;
            }
            int pos  = fileRec.getPath().lastIndexOf("/");
            String path = fileRec.getPath().substring(0,pos);
            File fileDir = new File(getSc().getBaseDir(),path);
            fileDir.mkdirs();
            byte[] buf = new byte[8192];
            File fullFile = new File(getSc().getBaseDir(),fileRec.getPath());
            FileOutputStream fos = new FileOutputStream(fullFile);
            int n;
            while((n = in.read(buf)) >= 0){
                fos.write(buf,0,n);
            }
            fos.close();
            in.close();
            setTxFinished(txid);
            LOG.info("filedownload fileId=" + fileId +", txid=" +txid);
        } catch (IOException e) {
            logger.error("txid:" + txid + ",url not downloaded:::" + internalUrl);
            setTxFinished(txid);
        }


    }


    synchronized  long writeAddFileLog(String fileId) throws IOException, ExecutionException, InterruptedException {
        long txid = getNextTxid();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        CommonTools.writeString(os, "add");
        CommonTools.writeString(os, fileId);
        byte[] data = os.toByteArray();
        LogRecord logRecord = new LogRecord();
        logRecord.setValue(data);
        txid = writeLog(logRecord);
        saveLocalVerion(txid);
        this.txid = txid;
        return txid;
    }

    synchronized long writeDeleteFileLog(String fileId) throws IOException, ExecutionException, InterruptedException {
        long txid = getNextTxid();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        CommonTools.writeString(os, "delete");
        CommonTools.writeString(os, fileId);
        byte[] data = os.toByteArray();
        LogRecord logRecord = new LogRecord();
        logRecord.setValue(data);
        txid = writeLog(logRecord);
        saveLocalVerion(txid);
        this.txid = txid;
        return txid;
    }


    @Override
    synchronized protected void updateLog(LogRecord logRec) {
        if (logRec.getOffset() > this.txid) {
            InputStream in = new ByteArrayInputStream(logRec.getValue());
            try {
                String action = CommonTools.readString(in);
                if(action.equals("add")){
                    String fileId = CommonTools.readString(in);
                    downloadFileService.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                downloadFile(fileId,logRec.getOffset());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
                else if(action.equals("delete")){
                    String fileId = CommonTools.readString(in);
                    deleteFile(fileId);
                    setTxFinished(logRec.getOffset());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }

    @Override
    protected long getLocalLastTxId() {
        return txid;
    }

    public void doSaveFile(InputStream is, OutputStream os) throws Exception {
        String fileId = CommonTools.readString(is);
        long fileLen = CommonTools.readLong(is);
        String filePath = getFilePathFromFileId(fileId);
        if (filePath == null) {
            throw new Exception("no file path");
        }
        File f = new File(getPath(filePath));
        f = new File(f.getAbsolutePath());
        f.mkdirs();
        f = new File(filePath);
//        System.out.println("saving file : " + f.getAbsolutePath());
        writeAddFileLog(fileId);
        FileOutputStream fos = new FileOutputStream(f.getAbsolutePath());
        while (fileLen > 0) {
            byte[] bytes = CommonTools.readBytes(is);
            if (bytes == null) {
                throw new Exception("file upload broken");
            }
            fileLen -= bytes.length;
            fos.write(bytes);
        }
        fos.close();
        CommonTools.writeString(os, "ok");
    }

    private String getFilePathFromFileId(String fileId) {
        //fileId是这样的g1_lg1_filesystemserver1@/2016/11/9/11522843278287989933854.jpg
        String[] parts = fileId.split("@");
        String path = parts[1];
        String serverName = parts[0];
        return baseDir + path;
    }

    private void deleteFile(String fileId) throws Exception {
        String filePath = getFilePathFromFileId(fileId);
        if (filePath == null) {
            throw new Exception("no file path");
        }
        File f = new File(filePath);
        f = new File(f.getAbsolutePath());
        f.delete();
    }

    public void doDelete(InputStream is, OutputStream os) throws Exception {
        try {

            String fileId = CommonTools.readString(is);
            writeDeleteFileLog(fileId);
            deleteFile(fileId);
            CommonTools.writeString(os, "ok");
        } catch (Exception e) {
            CommonTools.writeString(os, "error:" + e.getMessage());
        }
    }





    public void start() throws Exception {
        registerServerToZookeeper(getZk());
        Thread.sleep(1000);
//        writer = dlm.startLogSegmentNonPartitioned();


        File f = new File(getSc().getLogDir(), "ver.bin");
        long txid = 0;
        try {
            FileInputStream fis = new FileInputStream(f);
            txid = CommonTools.readLong(fis);
            logger.info("fileserver txid="+txid);

        }
        catch(Exception e){
            logger.warn(f.getAbsolutePath() + " 不存在，txid从0开始计算。");
        }
        this.txid = txid;
        httpClientManager.setConnection_timeout(3000);
        httpClientManager.setMax_connections_per_route(100);
        httpClientManager.setMax_total_connections(500);
        httpClientManager.init();

        finishedTxids = new TreeSet<>();
        downloadFileServerRecs();

        downloadFileServerRecsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        downloadFileServerRecs();
                        Thread.sleep(2000);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (JSONException e) {
                        e.printStackTrace();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            }
        }) {
        };

        downloadFileServerRecsThread.start();

        if (!checkMaster()) {
            startdownloadLogThread();
        } else {
            switchToMaster();
        }
        startCheckMasterThread();

    }

    public ByteArrayOutputStream handle(InputStream is, int flag) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            String action = CommonTools.readString(is);
            if (StringUtils.equals(action, "add")) {
                this.doSaveFile(is, out);
            } else if (StringUtils.equals(action, "delete")) {
                this.doDelete(is, out);
            } else {
                CommonTools.writeString(out, "error:unknow action '" + action + "'");
            }
        }catch(Exception e){
            CommonTools.writeString(out,"error:" + e.getMessage());
        }
        return out;
    }


}
