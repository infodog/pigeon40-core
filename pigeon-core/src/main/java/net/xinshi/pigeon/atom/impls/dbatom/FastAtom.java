package net.xinshi.pigeon.atom.impls.dbatom;

import net.xinshi.pigeon.atom.IServerAtom;
import net.xinshi.pigeon.cache.CacheLogger;
import net.xinshi.pigeon.persistence.IPigeonPersistence;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.status.Constants;
import net.xinshi.pigeon.util.CommonTools;
import net.xinshi.pigeon.util.DBUtils;
import net.xinshi.pigeon.util.DefaultHashGenerator;
import net.xinshi.pigeon.util.TimeTools;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.xinshi.pigeon.status.Constants.getStateString;

/**
 * Created by IntelliJ IDEA.
 * User: WPF
 * Date: 12-3-27
 * Time: 上午10:08
 * To change this template use File | Settings | File Templates.
 */

class AtomBean {
    String name;
    long value;
    long txid;
    int hash;
}

public class FastAtom implements IServerAtom, IPigeonPersistence {
    DataSource ds;
    String tableName;
    String versionTableName = "t_pigeontransaction";
    String versionKeyName;
    String logDirectory;
    PlatformTransactionManager txManager;
    boolean dbOK = true;
    boolean stateChange = true;
    Object stateMonitor;
    //Object flusherWaiter;
    boolean flusherStopped = true;
    int state_word = net.xinshi.pigeon.status.Constants.NORMAL_STATE;
    public VersionHistoryLogger verLogger;
    int maxCacheEntries = 100000;
    CacheLogger cacheLogger;
    boolean fastCreate = false;
    long savedbfailedcount = 0;
    Logger logger = LoggerFactory.getLogger(FastAtom.class);

    private String getCacheString() {
        return cacheLogger.getCacheString();
    }

    public Map getStatusMap() {
        Map<String, String> mapStatus = new HashMap<String, String>();
        mapStatus.put("state_word", getStateString(state_word));
        mapStatus.put("version", String.valueOf(verLogger.getVersion()));
//        if (verLogger.getLastRotate() > 0) {
//            mapStatus.put("last_rotate", (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(verLogger.getLastRotate()));
//        } else {
//            mapStatus.put("last_rotate", "");
//        }
        mapStatus.put("save_db_failed_count", String.valueOf(savedbfailedcount));
        mapStatus.put("cache_string", getCacheString());
        return mapStatus;
    }

    public int getMaxCacheEntries() {
        return maxCacheEntries;
    }

    public void setMaxCacheEntries(int maxCacheEntries) {
        this.maxCacheEntries = maxCacheEntries;
        // System.out.println("Atom maxCacheNumber = " + maxCacheEntries);
    }

    public boolean isFastCreate() {
        return fastCreate;
    }

    public void setFastCreate(boolean fastCreate) {
        this.fastCreate = fastCreate;
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getVersionTableName() {
        return versionTableName;
    }

    public void setVersionTableName(String versionTableName) {
        this.versionTableName = versionTableName;
    }

    public String getVersionKeyName() {
        return versionKeyName;
    }

    public void setVersionKeyName(String versionKeyName) {
        this.versionKeyName = versionKeyName;
    }

    public PlatformTransactionManager getTxManager() {
        return txManager;
    }

    public void setTxManager(PlatformTransactionManager txManager) {
        this.txManager = txManager;
    }

//    public String getLogDirectory() {
//        return logDirectory;
//    }

    public FastAtom() {
        //flusherWaiter = new Object();
        stateMonitor = new Object();
    }

    public void set_state_word(int state_word) throws Exception {
        this.state_word = state_word;
        if (state_word == Constants.NORMAL_STATE) {
            return;
        }
//        synchronized (flusherWaiter) {
//            flusherWaiter.notify();
//        }
        synchronized (stateMonitor) {
            stateChange = false;
        }
        if (flusherStopped) {
            return;
        }
        while (true) {
            synchronized (stateMonitor) {
                stateMonitor.wait(100);
                if (stateChange) {
                    break;
                }
            }
        }
    }

//    public void setLogDirectory(String logDirectory) {
//        if (!logDirectory.endsWith("/") && !logDirectory.endsWith("\\")) {
//            logDirectory = logDirectory + "/";
//        }
//        this.logDirectory = logDirectory;
//        File f = new File(logDirectory);
//        f = new File(f.getAbsolutePath());
//        this.logDirectory = f.getAbsolutePath();
//        if (!f.exists()) {
//            f.mkdirs();
//        }
//    }
//
//    private String getOperationLogFileName() {
//        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
//            logDirectory += "/";
//        }
//        return logDirectory + tableName + ".log";
//    }
//
//    private String getOldOperationLogFileName() {
//        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
//            logDirectory += "/";
//        }
//        return logDirectory + tableName + ".oldlog";
//    }



    @Override
    public long getVersion() {
        return verLogger.getVersion();
    }

    Object syncOnceLocker = new Object();

    public void syncVersion(long begin, long end) throws Exception {
        // do nothing
    }

//    public void writeLogAndDuplicate(String s, long txid) throws Exception {
//        if (!Constants.canWriteLog(state_word)) {
//            throw new Exception("pigeon is READONLY ...... ");
//        }
//        byte[] data = s.getBytes("UTF-8");
//        synchronized (cacheLogger) {
//            long ver = verLogger.logVersionHistory(data, cacheLogger.getLoggerFOS(), txid);
//            if (ver < 1) {
//                throw new Exception("logVersionHistory failed");
//            }
//            cacheLogger.flush();
//        }
//
//    }

    private void writeLog(String opname, String atomName, long opValue, long txid) throws Exception {
//        String line = opname + " " + atomName + " " + opValue + " " + txid + "\n";
//        writeLogAndDuplicate(line, txid);
        verLogger.setVersion(txid);
    }

    private void writeLog(String opname, String atomName, long testValue, long incValue, long txid) throws Exception {
//        String line = opname + " " + atomName + " " + testValue + " " + incValue + " " + txid + "\n";
//        writeLogAndDuplicate(line, txid);
        verLogger.setVersion(txid);
    }



//    private void deleteOldLog() {
//        File f = new File(getOldOperationLogFileName());
//        if (f.exists()) {
//            try {
//                if (!verLogger.rotateVersionHistory(this, f.getAbsolutePath())) {
//                    logger.error("atom rotateVersionHistory failed enter READONLY");
//                    set_state_word(Constants.READONLY_STATE);
//                    return;
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            f.delete();
//        }
//    }
//
//    private void deleteLog() {
//        File f = new File(getOperationLogFileName());
//        if (f.exists()) {
//            f.delete();
//        }
//    }

    private boolean flushSnapShotToDB() throws SQLException {

        long lastVersion = -1L;
        PlatformTransactionManager transactionManager = this.txManager;
        DefaultTransactionDefinition dtf = new DefaultTransactionDefinition();
        dtf.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        TransactionStatus ts = transactionManager.getTransaction(dtf);
        Connection conn = null;
        boolean isok = false;
        try {
            conn = ds.getConnection();
            if (cacheLogger.noSavingDirtyCache()) {
                verLogger.saveDbVersion(conn);
                isok = true;
                return true;
            }
            String updateSQL = null;
            String insertSQL = null;
            if(hasTxidInDB){
                updateSQL = String.format("update %s set value=? , txid=?, hash=? where name=? ", this.tableName);
                insertSQL = String.format("insert into %s (name,value,txid, hash)values(?,?,?,?) ", this.tableName);
            }
            else{
                updateSQL = String.format("update %s set value=? , hash=? where name=? ", this.tableName);
                insertSQL = String.format("insert into %s (name,value, hash)values(?,?,?) ", this.tableName);
            }
            PreparedStatement updateStmt = conn.prepareStatement(updateSQL);
            PreparedStatement insertStmt = conn.prepareStatement(insertSQL);
            for (Iterator it = cacheLogger.getSavingDirtyCache().entrySet().iterator(); it.hasNext(); ) {
                Map.Entry entry = (Map.Entry) it.next();
                AtomBean bean = (AtomBean) entry.getValue();
                String name = (String) entry.getKey();
                int hash = DefaultHashGenerator.hash(bean.name);
                if(hasTxidInDB) {
                    updateStmt.setLong(1, bean.value);
                    updateStmt.setLong(2, bean.txid);
                    updateStmt.setLong(3, hash);
                    updateStmt.setString(4, bean.name);
                    updateStmt.execute();
                    if (updateStmt.getUpdateCount() == 0) {
                        insertStmt.setString(1, name);
                        insertStmt.setLong(2, bean.value);
                        insertStmt.setLong(3, bean.txid);
                        insertStmt.setLong(4, hash);
                        insertStmt.execute();
                    }
                }
                else{
                    updateStmt.setLong(1, bean.value);
                    updateStmt.setLong(2, hash);
                    updateStmt.setString(3, bean.name);
                    updateStmt.execute();
                    if (updateStmt.getUpdateCount() == 0) {
                        insertStmt.setString(1, name);
                        insertStmt.setLong(2, bean.value);
                        insertStmt.setLong(3, hash);
                        insertStmt.execute();
                    }
                }
            }
            verLogger.saveDbVersion(conn);
            isok = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (isok) {
                transactionManager.commit(ts);
                synchronized (cacheLogger) {
                    cacheLogger.swapSavingToCache();
                }
            } else {
                transactionManager.rollback(ts);
                logger.error("fast atom error,rollback.");
            }
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
        if (lastVersion < 1) {
//            System.out.println("panic!!! atom (lastVersion < 1) = " + lastVersion);
            logger.error("panic!!! atom (lastVersion < 1) = " + lastVersion);
        }
        dbOK = isok;
        return isok;
    }

    AtomBean getBeanFromDB(String name) throws Exception {
        String sql = String.format("select * from %s where name=?", tableName);
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, name);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                AtomBean bean = new AtomBean();
                bean.txid = rs.getLong("txid");
                bean.value = rs.getLong("value");
                bean.name = rs.getString("name");
                bean.hash = rs.getInt("hash");
                return bean;
            } else {
                return null;
            }
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
    }

    private Long internalGet(String name) throws Exception {
        synchronized (cacheLogger) {
            AtomBean bean = (AtomBean) cacheLogger.getFromCaches(name);
            if(bean!=null){
                return bean.value;
            }
            else {
               bean = this.getBeanFromDB(name);
                if (bean != null) {
                    cacheLogger.putToCache(name, bean);
                    return bean.value;
                }
                else{
                    return null;
                }
            }
        }
    }

    public void addStringToCache(String line) throws Exception {
        synchronized (cacheLogger) {
            line = line.trim();
            line.replace("\n", "");
            try {
                String[] parts = line.split(" ");
                String op = parts[0];
                String name = parts[1];
                if (op.equals("createAndSet")) {
                    String value = parts[2];
                    long txid = Long.parseLong(parts[3]);
                    long lvalue = Long.parseLong(value);
                    Long v = this.internalGet(name);
                    if (!this.fastCreate && v != null) {
//                        System.out.println("[ignore] createAndSet atom " + name + " old value = " + v);
                        logger.warn("createAndSet atom " + name + " but atom already existed old value = " + v + ", operation ignored.");
                        return;
                    }
                    try {
                        AtomBean bean = new AtomBean();
                        bean.txid = txid;
                        bean.name = name;
                        bean.value = lvalue;
                        cacheLogger.putToDirtyCache(name, bean);
                    }
                    catch(Throwable e){
                        logger.info("error, line=" + line);
                        e.printStackTrace();
                    }
                } else if (op.equals("greaterAndInc")) {
                    Long v = this.internalGet(name);
                    String value = parts[2];
                    long testvalue = Long.parseLong(value);
                    long incvalue = Long.parseLong(parts[3]);
                    long txid = Long.parseLong(parts[4]);
                    if (v == null) {
                        return;
                    }
                    if (v > testvalue) {
                        v += incvalue;
                    }
                    AtomBean bean = new AtomBean();
                    bean.txid = txid;
                    bean.name = name;
                    bean.value = v;
                    cacheLogger.putToDirtyCache(name, bean);
                } else if (op.equals("lessAndInc")) {
                    Long v = this.internalGet(name);
                    String value = parts[2];
                    long testvalue = Long.parseLong(value);
                    long incvalue = Long.parseLong(parts[3]);
                    if (v == null) {
                        return;
                    }
                    if (v < testvalue) {
                        v += incvalue;
                    }
                    long txid = Long.parseLong(parts[4]);
                    AtomBean bean = new AtomBean();
                    bean.txid = txid;
                    bean.name = name;
                    bean.value = v;
                    cacheLogger.putToDirtyCache(name, bean);
                } else {
                    throw new Exception("not correct log format:" + line);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    private void executeFile(File f) throws Exception {
        if (f.exists() == false) {
            return;
        }
        long[] minmax = verLogger.getMinMaxVersion(f.getAbsolutePath());
        if (minmax == null) {
            return;
        }
        if (verLogger.getDbVersion() == minmax[1]) {
            throw new Exception("getMinMaxVersion (verLogger.getDbVersion() == minmax[1]) : "
                    + f.getAbsolutePath());
        }
        if (verLogger.getDbVersion() >= minmax[0]) {
            throw new Exception("getMinMaxVersion (verLogger.getDbVersion() >= minmax[0]) : "
                    + f.getAbsolutePath());
        }
        FileInputStream fis = new FileInputStream(f);
        try {
            while (true) {
                byte[] bytes = null;
                try {
                    bytes = verLogger.getBytesFromVersionHistoryFile(fis);
                    if (bytes == null) {
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
//                    System.out.println("atom !!!!!!!!! replay file failed : " + f.getAbsolutePath());
                    logger.error("atom !!!!!!!!! replay file failed : " + f.getAbsolutePath());
                    set_state_word(Constants.READONLY_STATE);
                    System.exit(-1);
                }
                String line = new String(bytes, "UTF-8");
                addStringToCache(line);
            }
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
    }



    boolean bInitialized = false;

    boolean hasTxidInDB; //检查数据库中是否有Txid字段


    public void init() throws Exception {
        if (!bInitialized) {
            bInitialized = true;
            boolean verInit = false;
            try {
                verLogger = new VersionHistoryLogger();
                verLogger.setLoggerDirectory(logDirectory + "/share");
                verLogger.setDs(ds);
                verLogger.setVersionTableName("t_pigeontransaction");
                verLogger.setVersionKeyName(versionKeyName);
                verInit = verLogger.init();
                cacheLogger = new CacheLogger();
//                cacheLogger.setNotification(flusherWaiter);
            } catch (Exception e) {
                e.printStackTrace();
                verInit = false;
            }
            if (!verInit) {
//                System.out.println("atom VersionHistoryLogger init failed");
                logger.error("atom VersionHistoryLogger init failed");
                set_state_word(Constants.READONLY_STATE);
                System.exit(-1);
            }
            hasTxidInDB = DBUtils.hasColumn(ds,"t_simpleatom","txid");
            beginFlusher();
            verLogger.reloadVersion();
        }
    }

    boolean noDirtyCache() {
        synchronized (cacheLogger) {
            if (state_word == Constants.NOWRITEDB_STATE && cacheLogger.noSavingDirtyCache()) {
                return true;
            }
            if (cacheLogger.noDirtyCache() && cacheLogger.noSavingDirtyCache()) {
                return true;
            }
            return false;
        }
    }

    void flush() {
        try {
            if (!dbOK) {
//                System.out.println("atom dbOK == false, flush()");
                logger.error("db is not ok.");
                return;
            }
            int rc = -1;
            try {
                rc = 0;
                cacheLogger.swapToSaving();
                if (flushSnapShotToDB()) {
                    savedbfailedcount = 0;
                } else {
                    savedbfailedcount++;
                }
            } catch (Exception e) {
                if (rc == -1) {
                    e.printStackTrace();
//                    System.out.println("atom swapToSavingAndRenameLog failed");
                    logger.error("atom swapToSavingAndRenameLog failed",e);
                    set_state_word(Constants.READONLY_STATE);
                }
                dbOK = false;
                savedbfailedcount++;
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void beginFlusher() {
        new Thread(new Flusher()).start();
    }

    class Flusher implements Runnable {
        public void run() {
            flusherStopped = false;
            Thread.currentThread().setName("FastAtom_Flusher_run");
            boolean waiting = true;
            long preTime = 0;
            String cacheString = "";
            while (true) {
                try {
                    if (preTime + 1000 * 600 < System.currentTimeMillis()) {
                        preTime = System.currentTimeMillis();
                        String cs = getCacheString();
                        if (cs.compareTo(cacheString) != 0) {
                            cacheString = cs;
//                            System.out.println(TimeTools.getNowTimeString() + " Atom " + cs);
                            logger.info(TimeTools.getNowTimeString() + " Atom " + cs);
                        }
                    }
                    if (waiting) {
//                        synchronized (flusherWaiter) {
//                            flusherWaiter.wait(1000);
//                        }
                        try {
                            if (cacheLogger.getDirtyCacheSize() == 0) {
                                Thread.sleep(1000);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        synchronized (stateMonitor) {
                            if (stateChange && !Constants.canWriteDB(state_word)) {
                                continue;
                            }
                        }
                    }
                    flush();
                    synchronized (stateMonitor) {
                        if (!stateChange) {
                            if (noDirtyCache()) {
                                stateChange = true;
                                stateMonitor.notify();
                                if (Constants.isStop(state_word)) {
                                    break;
                                }
                            } else {
                                waiting = false;
                            }
                        } else {
                            waiting = true;
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            flusherStopped = true;
        }
    }

    public void stop() throws InterruptedException {
    }

    public boolean createAndSet(String name, Integer initValue, long txid) throws Exception {
        synchronized (cacheLogger) {
            Long v = new Long(initValue);
            if (!this.fastCreate) {
                if (this.internalGet(name) != null) {
                    return false;
                }
            }
            writeLog("createAndSet", name, initValue, txid);
            cacheLogger.putToDirtyCache(name, v);
        }
        return true;
    }

    @Override
    public boolean greaterAndInc(String name, int testValue, int incValue, long txid) throws Exception {
        long rl = greaterAndIncReturnLong(name, testValue, incValue, txid);
        return true;
    }

    @Override
    public boolean lessAndInc(String name, int testValue, int incValue, long txid) throws Exception {
        long rl = lessAndIncReturnLong(name, testValue, incValue, txid);
        return true;
    }

    public long greaterAndIncReturnLong(String name, int testValue, int incValue, long txid) throws Exception {
        Long v = 0L;
        synchronized (cacheLogger) {
            v = this.internalGet(name);
            if (v == null) {
                throw new Exception(name + " does not exists!");
            }
            if (v >= testValue) {
                v += incValue;
                writeLog("greaterAndInc", name, testValue, incValue, txid);
                AtomBean bean = new AtomBean();
                bean.txid = txid;
                bean.name = name;
                bean.value = v;

                cacheLogger.putToDirtyCache(name, bean);
            } else {
                throw new Exception("return false");
            }
        }

        return v;
    }

    public long lessAndIncReturnLong(String name, int testValue, int incValue, long txid) throws Exception {
        Long v = 0L;
        synchronized (cacheLogger) {
            v = this.internalGet(name);
            if (v == null) {
                throw new Exception(name + " does not exists!");
            }
            if (v <= testValue) {
                v += incValue;
                writeLog("lessAndInc", name, testValue, incValue, txid);
                AtomBean bean = new AtomBean();
                bean.txid = txid;
                bean.name = name;
                bean.value = v;

                cacheLogger.putToDirtyCache(name, bean);
            } else {
                throw new Exception("return false");
            }
        }

        return v;
    }

    @Override
    public long getLastTxid() throws Exception {
        return verLogger.getVersion();
    }

    public Long get(String name) throws Exception {
        synchronized (cacheLogger) {
            return this.internalGet(name);
        }
    }

    public List<Long> getAtoms(List<String> atomIds) throws Exception {
        List<Long> result;
        result = new Vector<Long>();
        for (String id : atomIds) {
            result.add(internalGet(id));
        }
        return result;
    }

}

