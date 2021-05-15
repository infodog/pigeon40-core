package net.xinshi.pigeon.server.distributedserver;


import net.xinshi.pigeon.server.distributedserver.writeaheadlog.ILogManager;
import net.xinshi.pigeon.server.distributedserver.writeaheadlog.LogRecord;
//import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.json.JSONException;
import org.json.JSONObject;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;


/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-9
 * Time: 上午11:45
 * To change this template use File | Settings | File Templates.
 */

public abstract class BaseServer implements IServer, Watcher {

    String type;
    String nodeName;
    String instanceName;
    char role;
    String nodesString;
    static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String startTime;
    protected long writes = 0;
    protected long reads = 0;
    Logger logger = Logger.getLogger(BaseServer.class.getName());


    protected long txid = 0;
    protected String shardFullPath = "";
    private boolean isMaster = false;
    private boolean isSwitchingToMaster = false;
    private boolean isStopping = false;
    private boolean isSwitchingToSlave = false;

    private Object downloadThreadMonitor = new Object();

    private boolean isReaderError = true;

    ZooKeeper zk;
    String serverPath = null;
    ServerConfig sc;

    public ServerConfig getSc() {
        return sc;
    }


    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public void setSc(ServerConfig sc) {
        this.sc = sc;
    }

    public BaseServer() {
        startTime = this.dateFormat.format(new Date());
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public String getStartTime() {
        return startTime;
    }

    public char getRole() {
        return role;
    }

    public void setRole(char role) {
        this.role = role;
    }

    public String getNodesString() {
        return nodesString;
    }

    public void setNodesString(String nodesString) {
        this.nodesString = nodesString;
    }

    public String getShardFullPath() {
        return shardFullPath;
    }

    public void setShardFullPath(String shardFullPath) {
        this.shardFullPath = shardFullPath;
    }

    Thread downloadLogThread = null;
    Thread checkMasterThread = null;

    ILogManager logManager;

    public ILogManager getLogManager() {
        return logManager;
    }

    public void setLogManager(ILogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public void start() throws Exception {
        registerServerToZookeeper(getZk());
        Thread.sleep(1000);
//        writer = dlm.startLogSegmentNonPartitioned();
        if (!checkMaster()) {
            logger.info("going to start download thread : " + sc.shardFullPath);
            isMaster = false;
            startdownloadLogThread();
        } else {
            logger.info("on start, become:"+sc.shardFullPath);
            switchToMaster();
            System.out.println("on start switchedToMaster.");
        }
        startCheckMasterThread();
    }

    @Override
    public void stop() throws Exception {
        isStopping = true;
        if (downloadLogThread != null) {
            downloadLogThread.join();
            downloadLogThread = null;
        }
        if (checkMasterThread != null) {
            checkMasterThread.join();
            checkMasterThread = null;
        }
    }

    protected synchronized long getNextTxid() {
        long newTxid = getLocalLastTxId() + 1;
//        System.out.println("getNextTxid,newId=" + newTxid);
        return newTxid;
    }


    private void checkMasterAndSwitch() {
        try {
            boolean goMaster = checkMaster();
            if (goMaster && !isMaster) {
                switchToMaster();
            }
            if (!goMaster && isMaster) {
                System.out.println("check master failed, now switch to slave.");
                switchToSlave();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static public  String registerServerToZookeeper(ZooKeeper zk, ServerConfig sc) throws UnsupportedEncodingException, KeeperException, InterruptedException, JSONException {
        JSONObject jnodeData = new JSONObject(sc);
        jnodeData.put("shardExternalUrl",sc.getShardExternalUrl());
        jnodeData.put("shardInternalUrl",sc.getShardInternalUrl());
        jnodeData.put("externalUrl",sc.getExternalUrl());
        jnodeData.put("internalUrl",sc.getInternalUrl());
        jnodeData.remove("dbUrl");
        jnodeData.remove("dbUserName");
        jnodeData.remove("dbPassword");
        jnodeData.remove("driverClass");
        byte[] nodeData = jnodeData.toString().getBytes("utf-8");
        String serverPath = sc.shardFullPath + "/server_";
        System.out.println("registering " + serverPath);
        String actualServerPath = zk.create(serverPath, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("registered " + actualServerPath);
        return actualServerPath;
    }

    public String registerServerToZookeeper(ZooKeeper zk) throws UnsupportedEncodingException, KeeperException, InterruptedException, JSONException {
        this.serverPath = BaseServer.registerServerToZookeeper(zk, this.sc);
        return this.serverPath;
    }


    protected abstract void updateLog(LogRecord logRec);

    protected abstract long getLocalLastTxId();


    synchronized  void downloadLog() throws IOException {
        long nextTxId = getLocalLastTxId();
        logger.fine("downloadLog,nextTxId:" + nextTxId + ", " + sc.shardFullPath);
        logManager.seek(0,nextTxId);
        while (true) {
            List<LogRecord> logRecords = logManager.poll(Duration.ofSeconds(1));
            if(logRecords.size()>0) {
                logger.info("downloading logs， the logRecords.size=" + logRecords.size() + ", " + sc.shardFullPath);
            }
            for (LogRecord r : logRecords) {
                updateLog(r);
            }
            if (isSwitchingToMaster) {
                return;
            }
        }
    }

    protected long writeLog(LogRecord logRecord) throws IOException, ExecutionException, InterruptedException {
        logger.fine("writeLog:" + sc.shardFullPath);
        return logManager.writeLog(logRecord.getKey(),logRecord.getValue());
    }

    protected void startdownloadLogThread() throws IOException, InterruptedException {
        stopDownloadThread();

        isSwitchingToMaster = false;
        downloadLogThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    downloadLog();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(e.getCause());
                    System.exit(-1);
                }
            }
        }, "thread-download-log-" + shardFullPath);
        downloadLogThread.start();
    }

    protected void startCheckMasterThread() {
        checkMasterThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        checkMasterAndSwitch();
                        Thread.sleep(1000);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }

            }
        });
        checkMasterThread.start();

    }


    protected void switchToSlave() throws KeeperException, InterruptedException {
        this.isMaster = false;
        try {
            getZk().delete(this.serverPath, -1);
            System.out.println("switch to slave......");
            registerServerToZookeeper(getZk());
            startdownloadLogThread();
        } catch (Exception e) {
            //如果连switchtoSlave 都出错，直接退出了
            System.out.println("switchToSlave failed, exiting....");
            e.printStackTrace();
            System.exit(-1);
        }

    }

    synchronized  void stopDownloadThread() throws InterruptedException {
        if (downloadLogThread != null) {
            isSwitchingToMaster = true; //通知downloadThread to stop
            downloadLogThread.join(); //等待downloadThread to exit
        }
        downloadLogThread = null;
    }

    protected void switchToMaster() throws IOException, InterruptedException {
        //TODO:download logs, when we are caught up, setMaster(true)
//        setMaster(true);
        if(isMaster){
            return;
        }
        System.out.println("switching to master, please wait......");
        isSwitchingToMaster = true;

        stopDownloadThread();
        System.out.println("download thread stoped...");
        long lastShardTxId = 0;

        try {
            logger.info("before logManager.getLastOffset()");
            lastShardTxId = logManager.getLastOffset();
            logger.info("after logManager.getLastOffset, the lastShardTxId=" + lastShardTxId+ "," + sc.shardFullPath);
        } catch (Exception e) {
            System.out.println(this.shardFullPath + " log is empty.");
        }
        long localTxid = getLocalLastTxId();
        logger.info("localTxid=" + localTxid+"," + sc.shardFullPath);
        System.out.println("localTxid=" + localTxid+"," + sc.shardFullPath);
        if (lastShardTxId <= localTxid) {
            //成为了Master
            isSwitchingToMaster = false;

            setMaster(true);
            return;
        }
        //如果不是则下载日志到最新
        while (localTxid < lastShardTxId-1) {

            logManager.seek(0,localTxid);
            List<LogRecord> logs = logManager.poll(Duration.ofSeconds(2));
            logger.info("logs.size=" +logs.size()+"," + sc.shardFullPath);
            if(logs.size()==0){
                logger.severe("poll kafka got 0 records, localTxid=" + localTxid + ", lastShardTxId=" + lastShardTxId);
                break;

            }
            for(LogRecord r : logs){
                updateLog(r);
                long newlocalTxid = r.getOffset();
                if(newlocalTxid == localTxid){
                    //死循环了，直接跳出
                    logger.info("newlocalTxid ==  localTxid,可能出现死循环了。");
                    localTxid = lastShardTxId-1;
                    break;
                }
                localTxid = newlocalTxid;

            }

            logger.info("localTxid=" + localTxid+"," + sc.shardFullPath);
        }
        try {
            //现在再检查一下还是不是master
            if (checkMaster()) {
                isSwitchingToMaster = false;

                setMaster(true);

            } else {
                //不能成为master继续下载日志
                startdownloadLogThread();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected boolean checkMaster() throws KeeperException, InterruptedException {
        try {
//            logger.info("checking master:" + sc.shardFullPath);
            List<String> svrs = getZk().getChildren(sc.shardFullPath, this);
            Collections.sort(svrs);
            if (svrs.size() == 0) {
                //如果目前没有master,我们自己先注册自己作为master
                System.out.println("svrs.size==0,current server is " + this.serverPath);
                getZk().delete(this.serverPath, -1);
                registerServerToZookeeper(zk);
                System.out.println("after re register server, serverpath=" + this.serverPath);
                return true;
            }
            String svrName = svrs.get(0);
            String fullpath = sc.shardFullPath + "/" + svrName;
//            logger.info("fullpath=" + fullpath + ",this.serverPath="+this.serverPath);
            if (fullpath.equals(this.serverPath)) {
//                logger.info("true is master,fullpath=" + fullpath + ",this.serverPath="+this.serverPath);
                return true;
            } else {
//                logger.info("false is not master,fullpath=" + fullpath + ",this.serverPath="+this.serverPath);
                return false;
            }
        }
        catch(Exception e){
            return false;
        }

    }


    @Override
    public void joinCluster(ZooKeeper zk, ILogManager logManager) throws Exception {
//        this.zk = zk;
//        this.logManager = logManager;
//        this.serverPath = registerServerToZookeeper(zk);

        throw new Exception("not implemented.");

    }


    @Override
    public String getServerPath() {
        return this.serverPath;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean master) {

        isMaster = master;
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("process zookeeper event:" + event.toString() + ", return ......");

        /*try {
            if (checkMaster()) {
                switchToMaster();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}

