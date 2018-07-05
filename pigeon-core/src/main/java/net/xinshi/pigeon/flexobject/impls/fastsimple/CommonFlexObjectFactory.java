package net.xinshi.pigeon.flexobject.impls.fastsimple;


import net.xinshi.pigeon.flexobject.FlexObjectEntry;
import net.xinshi.pigeon.flexobject.IFlexObjectFactory;
import net.xinshi.pigeon.idgenerator.IIDGenerator;
import net.xinshi.pigeon.persistence.IPigeonPersistence;
import net.xinshi.pigeon.persistence.VersionHistoryLogger;
import net.xinshi.pigeon.status.Constants;
import net.xinshi.pigeon.util.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.xinshi.pigeon.status.Constants.getStateString;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-9-26
 * Time: 上午11:46
 * To change this template use File | Settings | File Templates.
 */
public class CommonFlexObjectFactory implements IFlexObjectFactory, IPigeonPersistence {
    Logger logger = LoggerFactory.getLogger(CommonFlexObjectFactory.class);
    Map cache = null;
    String logSaveDir;
    IIDGenerator idgenerator;
    long lastEntriesFlushedToDB;
    long lastFlushDuration;
    boolean stateChange = true;
    Object stateMonitor;
    //Object flusherWaiter;
    boolean flusherStopped = true;
    int state_word = net.xinshi.pigeon.status.Constants.NORMAL_STATE;
    public VersionHistoryLogger verLogger;
    String versionTableName = "t_pigeontransaction";
    String versionKeyName;
    int sizeToCompress = 512;
    int maxDirtyEntries = 100000;
    Map dirtyCache = Collections.synchronizedMap(new ConcurrentHashMap());
    Map olddirtycache = Collections.synchronizedMap(new ConcurrentHashMap());
    long maxContentLength = 1024000L;
    String tableName;
    String logDirectory;
    DataSource ds;
    PlatformTransactionManager txManager;
    int maxCacheNumber = 100000;
    FileOutputStream logfos = null;
    boolean bInitialized = false;
    int savedbfailedcount = 0;
    long merge_insert_count = 0;

    long txid;

   /* protected String merge_insert_sql = "INSERT INTO T_FLEXOBJECT(NAME, CONTENT, HASH, ISCOMPRESSED, ISSTRING,txid) VALUES (?, ?, ?, ?, ?,?) " +
            "ON DUPLICATE KEY UPDATE CONTENT=VALUES(CONTENT), HASH=VALUES(HASH), ISCOMPRESSED=VALUES(ISCOMPRESSED), ISSTRING=VALUES(ISSTRING),txid=VALUES(txid) ";*/

    String update_sql = "update T_FLEXOBJECT set CONTENT=?, ISCOMPRESSED=?,ISSTRING=?,HASH=?,txid=? where NAME=?";

    public CommonFlexObjectFactory() {
        //flusherWaiter = new Object();
        stateMonitor = new Object();
    }

    public Map getStatusMap() {
        Map<String, String> mapStatus = new HashMap<String, String>();
        mapStatus.put("state_word", getStateString(state_word));
        mapStatus.put("version", String.valueOf(verLogger.getVersion()));
        if (verLogger.getLastRotate() > 0) {
            mapStatus.put("last_rotate", (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(verLogger.getLastRotate()));
        } else {
            mapStatus.put("last_rotate", "");
        }
        mapStatus.put("save_db_failed_count", String.valueOf(savedbfailedcount));
        mapStatus.put("cache_string", getCacheString());
        return mapStatus;
    }

    private String getCacheString() {
        getCache();
        return "cache = " + cache.size() + ", dirtyCache = " + dirtyCache.size() + ", olddirtycache = " + olddirtycache.size();
    }

    public void setVersionKeyName(String versionKeyName) {
        this.versionKeyName = versionKeyName;
    }

    public void stop() throws InterruptedException {
        logger.info("stop = null");
    }

    public void set_state_word(int state_word) throws Exception {
        logger.info("CommonFlexObjectFactory set_state_word : " + state_word);
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

    public long getLastFlushDuration() {
        return lastFlushDuration;
    }

    public void setLastFlushDuration(long lastFlushDuration) {
        this.lastFlushDuration = lastFlushDuration;
    }

    public int getMaxDirtyEntries() {
        return maxDirtyEntries;
    }

    public void setMaxDirtyEntries(int maxDirtyEntries) {
        this.maxDirtyEntries = maxDirtyEntries;
    }

    public String getLogSaveDir() {
        return logSaveDir;
    }

    public void setLogSaveDir(String logSaveDir) {
        this.logSaveDir = logSaveDir;
    }

    void moveLog(String fileName) throws Exception {
        long version = idgenerator.getId("logfile");
        String newLog = logSaveDir + this.tableName + version + ".log";
        File logFile = new File(fileName);
        File newLogFile = new File(newLog);
        FileUtils.moveFile(logFile, newLogFile);
    }

    synchronized void putToDirtyCache(String name, FlexObjectEntry entry) throws Exception {
        dirtyCache.put(name, entry);
//        synchronized (flusherWaiter) {
//            flusherWaiter.notify();
//        }
    }

    public long getDirtyCacheSize() {
        return dirtyCache.size();
    }

    public IIDGenerator getIdgenerator() {
        return idgenerator;
    }

    public void setIdgenerator(IIDGenerator idgenerator) {
        this.idgenerator = idgenerator;
    }

    public long getMaxContentLength() {
        return maxContentLength;
    }

    public void setMaxContentLength(long maxContentLength) {
        this.maxContentLength = maxContentLength;
    }

    public int getMaxCacheNumber() {
        return maxCacheNumber;
    }

    public void setMaxCacheNumber(int maxCacheNumber) {
        this.maxCacheNumber = maxCacheNumber;
        // System.out.println("FlexObject maxCacheNumber = " + maxCacheNumber);
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public PlatformTransactionManager getTxManager() {
        return txManager;
    }

    public void setTxManager(PlatformTransactionManager txManager) {
        this.txManager = txManager;
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public void setLogDirectory(String logDirectory) {
        if (!logDirectory.endsWith("/") && !logDirectory.endsWith("\\")) {
            logDirectory = logDirectory + "/";
        }
        this.logDirectory = logDirectory;
        File f = new File(logDirectory);
        f = new File(f.getAbsolutePath());
        this.logDirectory = f.getAbsolutePath();
        if (!f.exists()) {
            f.mkdirs();
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public synchronized Map getCache() {
        if (cache == null) {
            cache = Collections.synchronizedMap(new SoftHashMap());
        }
        return cache;
    }

    String getLogFileName() {
        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
            logDirectory += "/";
        }
        return logDirectory + tableName + ".log";
    }

    String getOldLogFileName() {
        if (!logDirectory.endsWith("/") && (!logDirectory.endsWith("\\"))) {
            logDirectory += "/";
        }
        return logDirectory + tableName + ".oldlog";
    }

    void ensureLogfileOpen() throws Exception {
        if (logfos == null) {
            logfos = new FileOutputStream(getLogFileName(), true);
        }
    }

    private void saveToCache(FlexObjectEntry entry) {
        getCache();
        cache.put(entry.getName(), entry);
    }



    @Override
    public long getVersion() {
        return verLogger.getVersion();
    }

    Object syncOnceLocker = new Object();

    public void syncVersion(long begin, long end) throws Exception {
        //do nothing intentionally, do not sync in core
    }



    void writeLogAndCache(FlexObjectEntry entry) throws Exception {
        long ver = 0L;
        byte[] data = null;

        synchronized (this) {
            if (!Constants.canWriteLog(state_word)) {
                throw new Exception("pigeon is READONLY ...... ");
            }

            if(dirtyCache.size()>200000){
                throw new Exception("FlexObject writeLogAndCache too fast.dirtyCache.size()=" + dirtyCache.size());
            }

            ensureLogfileOpen();
            ByteArrayOutputStream baos = new ByteArrayOutputStream(5120);
            CommonTools.writeEntry(baos, entry);
            data = baos.toByteArray();
            ver = verLogger.logVersionHistory(data, logfos,entry.getTxid());
            if (ver < 1) {
                throw new Exception("VersionHistoryLogger version < 1 ...... ");
            }
            logfos.flush();
//            entry.setTxid(ver);
            if (entry != FlexObjectEntry.empty) {
                saveToCache(entry);
                putToDirtyCache(entry.getName(), entry);
            }

        }

    }

    public int getSizeToCompress() {
        return sizeToCompress;
    }

    public void setSizeToCompress(int sizeToCompress) {
        this.sizeToCompress = sizeToCompress;
    }

    public void saveContent(String name, String content) throws Exception {
        throw new Exception("deprecated, not supported any more.");
    }

    public List<String> getContents(List<String> names) throws Exception {
        Vector result = new Vector();
        for (String name : names) {
            String content = getContent(name);
            if (content == null) {
                content = "";
            }
            result.add(content);
        }
        return result;
    }

    @Override
    public void addContent(String name, String content) throws Exception {
        if (name.getBytes().length > 120) {
            throw new Exception("name too big, name = " + name);
        }
        if (content == null) {
            content = "";
        }
        boolean isCompressed = false;
        byte[] bytes = content.getBytes("UTF-8");
        if (bytes.length > this.sizeToCompress) {
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        if (bytes.length > maxContentLength) {
            throw new Exception("content too big for flexobjectclient");
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setAdd(true);
        entry.setBytesContent(bytes);
        entry.setName(name);
        entry.setHash(0);
        entry.setString(true);
        entry.setCompressed(isCompressed);
        writeLogAndCache(entry);
    }

    @Override
    public void addContent(String name, byte[] content) throws Exception {
        if (name.getBytes().length > 120) {
            throw new Exception("name too big, name = " + name);
        }
        if (content == null) {
            content = new byte[0];
        }
        boolean isCompressed = false;
        byte[] bytes = content;
        if (bytes.length > this.sizeToCompress) {
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        if (bytes.length > maxContentLength) {
            throw new Exception("content too big for flexobjectclient");
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setAdd(true);
        entry.setBytesContent(content);
        entry.setName(name);
        entry.setString(false);
        entry.setCompressed(isCompressed);
        writeLogAndCache(entry);
    }

    @Override
    public void saveFlexObject(FlexObjectEntry entry) throws Exception {
        writeLogAndCache(entry);
    }

    @Override
    public void saveBytes(String name, byte[] content) throws Exception {
        if (name.getBytes().length > 120) {
            throw new Exception("name too big, name = " + name);
        }
        if (content == null) {
            content = new byte[0];
        }
        boolean isCompressed = false;
        byte[] bytes = content;
        if (bytes.length > this.sizeToCompress) {
            bytes = CommonTools.zip(bytes);
            isCompressed = true;
        }
        if (bytes.length > maxContentLength) {
            throw new Exception("content too big for flexobjectclient");
        }
        FlexObjectEntry entry = new FlexObjectEntry();
        entry.setAdd(false);
        entry.setBytesContent(content);
        entry.setName(name);
        entry.setHash(0);
        entry.setString(false);
        entry.setCompressed(isCompressed);
        writeLogAndCache(entry);
    }

    @Override
    public int deleteContent(String name) throws Exception {
        saveContent(name, "");
        return 0;
    }

    @Override
    public List<FlexObjectEntry> getFlexObjects(List<String> names) throws Exception {
        List<FlexObjectEntry> result = new Vector<FlexObjectEntry>();
        for (String name : names) {
            FlexObjectEntry entry = getFlexObject(name);
            if (entry == null) {
                entry = FlexObjectEntry.empty;
            }
            result.add(entry);
        }
        return result;
    }

    @Override
    public FlexObjectEntry getFlexObject(String name) throws Exception {
        FlexObjectEntry result;
        FlexObjectEntry entry;
        entry = (FlexObjectEntry) getCache().get(name);
        if (entry == null) {
            entry = (FlexObjectEntry) dirtyCache.get(name);
        }
        if (entry == null) {
            entry = (FlexObjectEntry) olddirtycache.get(name);
        }
        if (entry == null) {
            result = getEntryFromDB(name);
            if (result != null) {
                result.setAdd(false);
                if (result != null) {
                    this.saveToCache(result);
                }
            }
            entry = result;
        }
        return entry;
    }

    @Override
    public byte[] getBytes(String name) throws Exception {
        FlexObjectEntry foe = null;
        try {
            foe = getFlexObject(name);
            return foe.getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void saveFlexObjects(List<FlexObjectEntry> objs) throws Exception {
        for (FlexObjectEntry entry : objs) {
            saveFlexObject(entry);
        }
    }

    void renameLog() throws Exception {
        File fo = new File(getOldLogFileName());
        if (fo.exists()) {
            throw new Exception("oldlog file exists , oldlog = " + fo.getAbsolutePath());
        }
        File f = new File(getLogFileName());
        if (f.exists()) {
            if (!f.renameTo(new File(getOldLogFileName()))) {
                throw new Exception("renameTo failed , name = " + getOldLogFileName());
            }
        }
    }

    synchronized void copydirtycache() {
        if (olddirtycache.size() > 0) {
            return;
        }
        if (dirtyCache.size() > 0) {
            olddirtycache.putAll(dirtyCache);
            dirtyCache.clear();
        }
    }

    synchronized boolean noDirtyCache() {
        if (state_word == Constants.NOWRITEDB_STATE && olddirtycache.size() == 0) {
            return true;
        }
        if (dirtyCache.size() == 0 && olddirtycache.size() == 0) {
            return true;
        }
        return false;
    }

    synchronized void takeSnapShot() throws Exception {
        if (olddirtycache.size() < 1 && dirtyCache.size() > 0) {
            if (logfos != null) {
                logfos.close();
                logfos = null;
            }
            renameLog();
            copydirtycache();
        }
    }



    public String getContent(String name) throws Exception {
        FlexObjectEntry entry = getFlexObject(name);
        if (entry == null) {
            return null;
        }
        return entry.getContent();
    }

    public String getConstant(String name) throws Exception {
        throw new Exception("not implement getConstant() ...... ");
    }

    private FlexObjectEntry getEntryFromDB(String name) throws SQLException {
        FlexObjectEntry result = new FlexObjectEntry();
        String sql = "select * from " + tableName + " where name=?";
        Connection conn = null;
        try {
            conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                byte[] bytes = rs.getBytes("content");
                result.setBytesContent(bytes);
                result.setCompressed(rs.getBoolean("isCompressed"));
                result.setString(rs.getBoolean("isString"));
                result.setName(name);
                result.setTxid(rs.getLong("txid"));
                result.setHash(rs.getLong("hash"));

                rs.close();
                stmt.close();
                return result;
            } else {
                rs.close();
                stmt.close();
                return null;
            }
        } finally {
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
    }

    void deleteOldLog() throws Exception {
        boolean isOK = false;
        try {
            if (verLogger.rotateVersionHistory(this, this.getOldLogFileName())) {
                deleteLog(this.getOldLogFileName());
                isOK = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!isOK) {
            logger.warn("rotateVersionHistory failed enter READONLY ... ");
            set_state_word(Constants.READONLY_STATE);
        }
    }

    void deleteLog(String fileName) throws Exception {
        if (StringUtils.isBlank(this.logSaveDir)) {
            File f = new File(fileName);
            if (f.exists()) {
                f.delete();
            }
        } else {
            this.moveLog(fileName);
        }
    }

    void flush() throws Exception {
        takeSnapShot();
        if (olddirtycache.size() == 0) {
            return;
        }
        putOldDirtyBandToDB();
    }

    private boolean saveEntries(Collection<FlexObjectEntry> entries) throws Exception {

//        long lastVersion = -1L;
        long begin = System.currentTimeMillis();
        long count = entries.size();

        String deleteSQL = "delete from " + tableName + " where name=?";
        Connection conn = null;
        PlatformTransactionManager transactionManager = this.txManager;
        DefaultTransactionDefinition dtf = new DefaultTransactionDefinition();
        dtf.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        TransactionStatus ts = transactionManager.getTransaction(dtf);
        List<FlexObjectEntry> updateEntries = new Vector<FlexObjectEntry>();
        List<FlexObjectEntry> deleteEntries = new Vector<FlexObjectEntry>();
        boolean isOK = false;
        try {
            conn = ds.getConnection();
            if (entries.size() < 1) {
                verLogger.saveDbVersion(conn);
                isOK = true;
                return true;
            }
//            PreparedStatement updateStmt = conn.prepareStatement(merge_insert_sql);
            PreparedStatement deleteStmt = conn.prepareStatement(deleteSQL);
            for (FlexObjectEntry en : entries) {
                logger.debug(en.getName());
                if (en.getBytesContent().length < 1) {
                    deleteEntries.add(en);
                } else {
                    updateEntries.add(en);
                }
            }
            if (!doUpdateToDB(updateEntries, conn, null)) {
                return false;
            }
            doDeleteFromDB(deleteEntries, deleteStmt);
            verLogger.saveDbVersion(conn);
            isOK = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (isOK) {
                txManager.commit(ts);
                verLogger.setDbVersion(verLogger.getVersion());
                synchronized (this) {
                    olddirtycache.clear();
                }
                deleteOldLog();
            } else {
                txManager.rollback(ts);
//                System.out.println(" CommonFlexObjectFactory rollback.");
                logger.error("FlexObject:System error,can not insert into mysql......");
            }
            if (conn != null && conn.isClosed() == false) {
                conn.close();
            }
        }
        long end = System.currentTimeMillis();
        logger.info("flexobject flush to database,  " + count + " entries, takes " + (end-begin) + "ms" );
        return isOK;
    }

    private void putOldDirtyBandToDB() throws Exception {
        this.lastEntriesFlushedToDB = olddirtycache.size();
        logger.info("olddirtycache.size()=" + olddirtycache.size());
        long begin = System.currentTimeMillis();
        if (!saveEntries(olddirtycache.values())) {
            ++savedbfailedcount;
            logger.warn("saveEntries failed, for " + savedbfailedcount + "times");
            if (savedbfailedcount > 1800) {
                logger.warn("saveEntries failed, for savedbfailedcount > 1800 enter READONLY");
                //System.out.println("putOldDirtyBandToDB savedbfailedcount > 1800 enter READONLY");
                set_state_word(Constants.READONLY_STATE);
            }
        } else {
            this.savedbfailedcount = 0;
        }
        long end = System.currentTimeMillis();
        this.lastFlushDuration = end - begin;
        // System.out.println(TimeTools.getNowTimeString() + " FlexObject lastFlushDuration = " + lastFlushDuration);
        logger.info("putOldDirtyBandToDB took " + lastFlushDuration + "ms");
    }

    private void doDeleteFromDB(List<FlexObjectEntry> deleteEntries, PreparedStatement deleteStmt) throws Exception {
        int n = 0;
        if (deleteEntries.size() < 1) {
            return;
        }
        for (FlexObjectEntry entry : deleteEntries) {
            deleteStmt.setBytes(1, entry.getName().getBytes("utf-8"));
            deleteStmt.addBatch();
            if (++n % 2000 == 0) {
                deleteStmt.executeBatch();
            }
        }
        deleteStmt.executeBatch();
    }

    private boolean splitEntries(List<FlexObjectEntry> updateEntries, Connection conn, List<FlexObjectEntry> existedEntries, List<FlexObjectEntry> newEntries) throws Exception {
        long begin = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        sb.append("select name from " + tableName + " where name in (");
        Statement stmt = conn.createStatement();
        HashMap existedNames = new HashMap();
        boolean isOK = true;
        boolean first = true;
        try {
            for (FlexObjectEntry entry : updateEntries) {
                if (!first) {
                    sb.append(",");
                } else {
                    first = false;
                }
                sb.append("x'").append(Hex.encodeHex(entry.getName().getBytes("utf8"))).append("'");
                if (sb.length() > 2 * 1024 * 1024) {
                    sb.append(")");
                    ResultSet r = stmt.executeQuery(sb.toString());
                    while (r.next()) {
                        byte[] bytes = r.getBytes(1);
                        String name = new String(bytes, "utf8");
                        existedNames.put(name, name);
                    }
                    r.close();
                    sb = new StringBuilder();
                    sb.append("select name from " + tableName + " where name in (");
                    first = true;
                }
            }
            if (!first) {
                sb.append(")");
                ResultSet r = stmt.executeQuery(sb.toString());
                while (r.next()) {
                    byte[] bytes = r.getBytes(1);
                    String name = new String(bytes, "utf8");
                    existedNames.put(name, name);
                }
                r.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            isOK = false;
        } finally {
            stmt.close();
        }
        for (FlexObjectEntry entry : updateEntries) {
            if (existedNames.containsKey(entry.getName())) {
                existedEntries.add(entry);
            } else {
                newEntries.add(entry);
            }
        }
        long end = System.currentTimeMillis();
        return isOK;
    }

    private boolean doUpdateToDB(List<FlexObjectEntry> updateEntries, Connection conn, PreparedStatement unUsed) throws Exception {
        if (updateEntries.size() < 1) {
            return true;
        }
        List<FlexObjectEntry> existedEntries = new Vector<FlexObjectEntry>();
        List<FlexObjectEntry> newEntries = new Vector<FlexObjectEntry>();
        if (!splitEntries(updateEntries, conn, existedEntries, newEntries)) {
//            System.out.println("doUpdateToDB.splitEntries() == false");
            logger.error("doUpdateToDB.splitEntries() == false");
            return false;
        }

        PreparedStatement updateStmt = conn.prepareStatement(update_sql);

        try{
            if (!realUpdateToDB(existedEntries, conn, updateStmt)) {
               logger.error("doUpdateToDB.realUpdateToDB() == false");
                return false;
            }
            if (!doAddToDB(newEntries, conn, null)) {
                logger.error("doUpdateToDB.doAddToDB() == false");
                return false;
            }
            return true;
        }
        finally{
            updateStmt.close();
        }

    }

    private boolean realUpdateToDB(List<FlexObjectEntry> updateEntries, Connection conn, PreparedStatement updateStmt) throws Exception {
        try {
            int n = 0;
            List<FlexObjectEntry> newEntries = new Vector<FlexObjectEntry>();
            for (FlexObjectEntry entry : updateEntries) {
                int hash = DefaultHashGenerator.hash(entry.getName());
                entry.setHash(hash);
                if(hasTxidInDB) {
                    updateStmt.setBytes(1, entry.getBytesContent());
                    updateStmt.setBoolean(2, entry.isCompressed());
                    updateStmt.setBoolean(3, entry.isString());
                    updateStmt.setLong(4, entry.getHash());
                    updateStmt.setLong(5, entry.getTxid());
                    updateStmt.setBytes(6, entry.getName().getBytes("utf-8"));
                }
                else{
                    updateStmt.setBytes(1, entry.getBytesContent());
                    updateStmt.setBoolean(2, entry.isCompressed());
                    updateStmt.setBoolean(3, entry.isString());
                    updateStmt.setLong(4, entry.getHash());
                    updateStmt.setBytes(5, entry.getName().getBytes("utf-8"));
                }
                updateStmt.addBatch();
                if (++n % 2000 == 0) {
                    int[] updateCounts = updateStmt.executeBatch();
                    updateStmt.clearBatch();
                }
            }
            int[] updateCounts = updateStmt.executeBatch();
            updateStmt.clearBatch();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean doAddToDB(List<FlexObjectEntry> addEntries, Connection conn, PreparedStatement updateStmt) throws Exception {
        boolean isOK = true;
        if (addEntries.size() == 0) {
            return isOK;
        }
        Statement stmt = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        List<FlexObjectEntry> roundEntries = new ArrayList<FlexObjectEntry>();
        sb.append("insert into " + tableName + "(name,content,hash,isCompressed,isString,txid) values");
        boolean first = true;
        try {
            for (FlexObjectEntry entry : addEntries) {
                int hash = DefaultHashGenerator.hash(entry.getName());
                entry.setHash(hash);
                roundEntries.add(entry);
                StringBuilder sb1 = new StringBuilder();
                sb1.append("(").append("x'").append(Hex.encodeHex(entry.getName().getBytes("utf-8"))).append("',");
                sb1.append("x'").append(Hex.encodeHex(entry.getBytesContent())).append("',");
                sb1.append(entry.getHash()).append(",");
                sb1.append(entry.isCompressed() ? 1 : 0).append(",");
                sb1.append(entry.isString() ? 1 : 0).append(",");
                sb1.append(entry.getTxid()).append(")");
                if (!first) {
                    sb.append(",");
                } else {
                    first = false;
                }
                sb.append(sb1);
                if (sb.length() > 1024 * 1024 * 4) {
                    try {
                        stmt.execute(sb.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error("doAddToDB() failed",e);
                        return false;
                    }
                    roundEntries.clear();
                    first = true;
                    sb = new StringBuilder();
                    sb.append("insert into " + tableName + "(name,content,hash,isCompressed,isString) values");
                }
            }
            if (first == false) {
                try {
                    stmt.execute(sb.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("catch (...) 22 ------------------- doAddToDB() failed");
                    return false;
                }
            }
        } finally {
            stmt.close();
        }
        return isOK;
    }



    void replay() throws Exception {
        replayFile(this.getOldLogFileName());
        renameLog();
        replayFile(this.getOldLogFileName());
    }

    void replayFile(String filename) throws Exception {
        File f = new File(filename);
        if (!f.exists()) {
            return;
        }
        FileInputStream fis = new FileInputStream(f);
        try {
            Map<String, FlexObjectEntry> mapEntries = new HashMap<String, FlexObjectEntry>();
            while (true) {
                InputStream mis = null;
                try {
                    mis = verLogger.getInputStreamFromVersionHistoryFile(fis);
                    if (mis == null) {
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error( "!!!!!!!!! replay file failed : " + filename,e);
                    set_state_word(Constants.READONLY_STATE);
                    System.exit(-1);
                }
                FlexObjectEntry entry = CommonTools.readEntry(mis);
                if (entry == null) {
                    break;
                }
                verLogger.setVersion(entry.getTxid());
                mapEntries.put(entry.getName(), entry);
            }
            fis.close();
            fis = null;
            if (saveEntries(mapEntries.values())) {
                deleteOldLog();
            } else {
                logger.error("!!!!!!!!! replay file failed : " + filename);
                set_state_word(Constants.READONLY_STATE);
                System.exit(-1);
            }
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
    }



    class Flusher implements Runnable {
        Logger logger = LoggerFactory.getLogger(Flusher.class);
        public void run() {
            flusherStopped = false;
            Thread.currentThread().setName("CommonFlexObjectFactory_Flusher_run");
            boolean waiting = true;
            long preTime = 0;
            String cacheString = "";
            while (true) {
                try {
                    if (preTime + 1000 * 600 < System.currentTimeMillis()) {
                        //已经有600秒没有执行了
                        preTime = System.currentTimeMillis();
                        String cs = getCacheString();
                        if (cs.compareTo(cacheString) != 0) {
                            cacheString = cs;
                            logger.info(cs);
                            //System.out.println(TimeTools.getNowTimeString() + " FlexObject " + cs);
                        }
                    }
                    if (waiting) {
//                        synchronized (flusherWaiter) {
//                            flusherWaiter.wait(1000);
//                        }
                        try {
                            if (dirtyCache.size() == 0) {
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
    boolean hasTxidInDB; //检查数据库中是否有Txid字段
    public void init() throws Exception {
        if (bInitialized) {
            return;
        } else {
            bInitialized = true;
        }
        boolean verInit = false;
        try {
            verLogger = new VersionHistoryLogger();
            verLogger.setLoggerDirectory(logDirectory + "/share");
            verLogger.setDs(ds);
            verLogger.setVersionTableName("t_pigeontransaction");
            verLogger.setVersionKeyName(versionKeyName);
//            merge_insert_sql = merge_insert_sql.replace("T_FLEXOBJECT", tableName);
            verInit = verLogger.init();
        } catch (Exception e) {
            e.printStackTrace();
            verInit = false;
        }
        if (!verInit) {
            logger.error("VersionHistoryLogger init failed");
            set_state_word(Constants.READONLY_STATE);
            System.exit(-1);
        }
        hasTxidInDB = DBUtils.hasColumn(ds,"t_flexobject","txid");
        if(hasTxidInDB){
            update_sql = "update T_FLEXOBJECT set CONTENT=?, ISCOMPRESSED=?,ISSTRING=?,HASH=?,txid=? where NAME=?";
        }
        else{
            update_sql = "update T_FLEXOBJECT set CONTENT=?, ISCOMPRESSED=?,ISSTRING=?,HASH=? where NAME=?";
        }
        this.replay();
        new Thread(new Flusher()).start();
        //重新读取数据库中的version,现在应该是dbVersion为准
        verLogger.reloadVersion();
    }

    @Override
    public void setTlsMode(boolean open) {
    }

    @Override
    public void saveTemporaryContent(String name, String content) throws Exception {
    }

    @Override
    public long getLastTxid() throws Exception {
        return verLogger.getVersion();
    }

}


