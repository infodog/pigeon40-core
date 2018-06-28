package net.xinshi.pigeon.server.distributedserver;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.LogWriter;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.zookeeper.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.omg.PortableServer.THREAD_POLICY_ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    private static final Logger LOG = LoggerFactory.getLogger(BaseServer.class);

    protected long txid = 0;
    protected String shardFullPath = "";
    private boolean isMaster = false;
    private boolean isSwitchingToMaster = false;
    private boolean isStopping = false;
    private boolean isSwitchingToSlave = false;

    private Object downloadThreadMonitor = new Object();

    private boolean isReaderError = true;

    ZooKeeperClient ztc;

    public ZooKeeper getZk() throws InterruptedException, ZooKeeperClient.ZooKeeperConnectionException {
        return ztc.get();
    }


    public DistributedLogManager getDlm() {
        return dlm;
    }

    public void setDlm(DistributedLogManager dlm) {
        this.dlm = dlm;
    }

    DistributedLogManager dlm;

    String serverPath = null;


    ServerConfig sc;

    public ServerConfig getSc() {
        return sc;
    }

    public ZooKeeperClient getZtc() {
        return ztc;
    }

    public void setZtc(ZooKeeperClient ztc) {
        this.ztc = ztc;
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

    LogWriter writer = null;

    protected LogWriter getWriter() throws IOException {
        if (writer != null) {
            return writer;
        }
        writer = dlm.openLogWriter();
        return writer;
    }


    public void start() throws Exception {
        registerServerToZookeeper(getZk());
        Thread.sleep(1000);
//        writer = dlm.startLogSegmentNonPartitioned();
        if (!checkMaster()) {
            startdownloadLogThread();
        } else {
            switchToMaster();
        }
        startCheckMasterThread();
    }

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
        System.out.println("getNextTxid,newId=" + newTxid);
        return newTxid;
    }


    private void checkMasterAndSwitch() {
        try {
            boolean goMaster = checkMaster();
            if (goMaster && !isMaster) {
                switchToMaster();
            }
            if (!goMaster && isMaster) {
                switchToSlave();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String registerServerToZookeeper(ZooKeeper zk, ServerConfig sc) throws UnsupportedEncodingException, KeeperException, InterruptedException, JSONException {
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
        String actualServerPath = zk.create(serverPath, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        return actualServerPath;
    }

    public String registerServerToZookeeper(ZooKeeper zk) throws UnsupportedEncodingException, KeeperException, InterruptedException, JSONException {
        this.serverPath = registerServerToZookeeper(zk, this.sc);
        return this.serverPath;
    }


    protected abstract void updateLog(LogRecord logRec);

    protected abstract long getLocalLastTxId();


    void downloadLog() throws IOException {
        long nextTxId = getLocalLastTxId();
        long lastShardTxId = 0;
        try {
            lastShardTxId = dlm.getLastTxId();

        } catch (Throwable t) {
            //当日志为空的时候，dlm.getLastTxId会出异常，所以需要try住
            //do nothing
        }
        LogReader reader = null;

        while (true) {
            try {
                try {
                    lastShardTxId = dlm.getLastTxId();
//                    LOG.warn(this.nodeName + "," + this.instanceName + ":" + "lastShardTxid:" + lastShardTxId + ",localLastTxId:" + getLocalLastTxId());

                } catch (Throwable t) {
                    //当日志为空的时候，dlm.getLastTxId会出异常，所以需要try住
                    //do nothing
                }
                if(lastShardTxId>nextTxId) {
                    reader = dlm.getInputStream(nextTxId);
                    while (true) {
                        try {
                            LogRecord record = reader.readNext(false);
                            if (record == null) {
                                reader.close();
                                reader = null;
                                break;
                            }
//                        System.out.println("read record, txid=" + record.getTransactionId() + ", lastShardId=" + lastShardTxId);
                            if (record.getTransactionId() > getLocalLastTxId()) {
                                LOG.warn("updatelog " + this.nodeName + "," + this.instanceName + ":" + "newTransactionId:" + record.getTransactionId() + ",localLastTxId:" + getLocalLastTxId());
                                updateLog(record);
                            }
                            // read next record
                        } catch (LogEmptyException t) {
                            //没事
                            System.out.println("no record in log.");
                        } catch (Throwable ioe) {
                            // handle the exception
                            nextTxId = getLocalLastTxId();
                            reader = dlm.getInputStream(nextTxId);
                        }

                    }
                }




                if (isSwitchingToMaster) {
                    return;
                }
                synchronized (downloadThreadMonitor) {
                    downloadThreadMonitor.wait(100);
                }
                if (isSwitchingToMaster) {
                    return;
                }




            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    protected long writeLog(long txid, byte[] logData) throws IOException {
        LogRecord logRecord = new LogRecord(txid, logData);
        LogWriter logWriter = getWriter();
        logWriter.write(logRecord);
        logWriter.flush();
        logWriter.commit();
        return txid;
    }

    protected void startdownloadLogThread() throws IOException {
        isSwitchingToMaster = false;
        if (writer != null) {
            writer.close();
            writer = null;
        }
        downloadLogThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    downloadLog();
                } catch (Exception e) {
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
            registerServerToZookeeper(getZk());
            if (writer != null) {
                try {
                    writer.close();
                    writer = null;
                } catch (Throwable t) {
                    System.out.println("warning: exception when switch to slave.");
                }
            }
            startdownloadLogThread();
        } catch (Exception e) {
            //如果连switchtoSlave 都出错，直接退出了
            System.out.println("switchToSlave failed, exiting....");
            e.printStackTrace();
            System.exit(-1);
        }

    }


    protected void switchToMaster() throws IOException, InterruptedException {
        //TODO:download logs, when we are caught up, setMaster(true)
//        setMaster(true);
        synchronized (downloadThreadMonitor) {
            if (downloadLogThread != null) {
                isSwitchingToMaster = true; //通知downloadThread to stop
                downloadThreadMonitor.notify();
                downloadLogThread.join(); //等待downloadThread to exit
            }
        }
        long lastShardTxId = 0;

        try {
            lastShardTxId = dlm.getLastTxId();
        } catch (Exception e) {
            System.out.println(this.shardFullPath + " log is empty.");
        }
        long localTxid = getLocalLastTxId();
        if (lastShardTxId <= localTxid) {
            //成为了Master
            setMaster(true);
            return;
        }
        //如果不是则下载日志到最新
        LogReader reader;
        reader = dlm.getInputStream(localTxid);
        while (localTxid < lastShardTxId) {
            try {
                // handle the exception
//                localTxid = getLocalLastTxId();

                LogRecord record;
                record = reader.readNext(false);
                if (record.getTransactionId() > getLocalLastTxId()) {
                    updateLog(record);
                }
                localTxid = record.getTransactionId();
                if (localTxid == lastShardTxId) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                localTxid = getLocalLastTxId();
                reader = dlm.getInputStream(localTxid);
            }
        }

        try {
            //现在再检查一下还是不是master
            if (checkMaster()) {
                writer = dlm.openLogWriter();
                setMaster(true);
            } else {
                //不能成为master继续下载日志
                startdownloadLogThread();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected boolean checkMaster() throws KeeperException, InterruptedException, ZooKeeperClient.ZooKeeperConnectionException {
        try {
            List<String> svrs = getZk().getChildren(sc.shardFullPath, this);
            Collections.sort(svrs);
            if (svrs.size() == 0) {
                return false;
            }
            String svrName = svrs.get(0);
            String fullpath = sc.shardFullPath + "/" + svrName;
            if (fullpath.equals(this.serverPath)) {
//            System.out.println(fullpath + " is master.");
                return true;
            } else {
//            System.out.println(fullpath + " is slave.");
                return false;
            }
        }
        catch(IOException e){
            return false;
        }

    }


    @Override
    public void joinCluster(ZooKeeperClient ztc, DistributedLogManager dlm) throws Exception {
        this.ztc = ztc;
        this.dlm = dlm;
        this.serverPath = registerServerToZookeeper(ztc.get());
        ztc.registerExpirationHandler(new ZooKeeperClient.ZooKeeperSessionExpireNotifier() {
            @Override
            public void notifySessionExpired() {
                try {
                    registerServerToZookeeper(ztc.get());
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
        try {
            if (checkMaster()) {
                switchToMaster();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

